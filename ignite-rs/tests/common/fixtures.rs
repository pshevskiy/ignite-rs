use bollard::container::{
    Config as DockerContainerConfig, CreateContainerOptions, ListContainersOptions,
    NetworkingConfig, RemoveContainerOptions, StartContainerOptions, StopContainerOptions,
};
use bollard::image::CreateImageOptions;
use bollard::models::{EndpointIpamConfig, EndpointSettings, HostConfig, PortBinding};
use bollard::network::{ConnectNetworkOptions, CreateNetworkOptions};
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
use std::fs::{self, OpenOptions};
use std::future::Future;
use std::io::{self, ErrorKind, Write};
use std::net::{Ipv4Addr, Shutdown, TcpListener, TcpStream};
use std::ops::Deref;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread;
use std::time::{Duration, Instant, SystemTime};
use testcontainers::{GenericImage, ImageArgs, RunnableImage};
use tokio::runtime::{Builder as TokioRuntimeBuilder, Handle as TokioHandle};

const DEFAULT_IGNITE_IMAGE: &str = "apacheignite/ignite";
const DEFAULT_IGNITE_TAG: &str = "2.15.0";
const DEFAULT_LOCK_WAIT_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_LOCK_STALE_AFTER: Duration = Duration::from_secs(300);
const LOCK_RETRY_DELAY: Duration = Duration::from_millis(100);
const CONTAINER_STATE_POLL_DELAY: Duration = Duration::from_millis(100);
const CONTAINER_STOP_TIMEOUT: Duration = Duration::from_secs(15);
const CONTAINER_START_TIMEOUT: Duration = Duration::from_secs(15);
const DOCKER_API_TIMEOUT: Duration = Duration::from_secs(30);
const NETWORK_CREATE_SETTLE_TIMEOUT: Duration = Duration::from_secs(45);
const CLUSTER_READY_TIMEOUT: Duration = Duration::from_secs(240);
const CHURN_RESET_READY_TIMEOUT: Duration = Duration::from_secs(20);
const READY_PROBE_TIMEOUT: Duration = Duration::from_secs(5);
const IGNITE_PORT: u16 = 10800;
const CLUSTER_LAYOUT_VERSION: &str = "4";
const CLUSTER_3_BASE_PORT: u16 = 12100;
const CLUSTER_3_CHURN_BASE_PORT: u16 = 12400;
const DEFAULT_START_RETRIES: usize = 40;
const DEFAULT_START_DELAY_MS: u64 = 500;
const BASE_CLUSTER_NODE_COUNT: usize = 3;
const DISCOVERY_CLUSTER_NODE_COUNT: usize = 4;
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
static DOCKER_BACKEND: OnceLock<ManagedDockerBackend> = OnceLock::new();
static PROCESS_CLEANUP_READY: OnceLock<()> = OnceLock::new();
#[cfg(unix)]
static SIGNAL_CLEANUP_READY: OnceLock<()> = OnceLock::new();
#[cfg(unix)]
static SIGNAL_PIPE_WRITE_FD: AtomicI32 = AtomicI32::new(-1);
static PROCESS_CLEANUP_RUNNING: AtomicBool = AtomicBool::new(false);

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

#[derive(Clone)]
struct ManagedDockerBackend {
    docker: Docker,
}

#[derive(Clone, Debug)]
pub struct IgniteContext {
    profile: IgniteProfile,
    scope: FixtureScope,
    kind: IgniteContextKind,
}

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

#[cfg(unix)]
extern "C" {
    fn atexit(cb: extern "C" fn()) -> i32;
    fn kill(pid: i32, sig: i32) -> i32;
    fn pipe(fds: *mut i32) -> i32;
    fn read(fd: i32, buf: *mut u8, count: usize) -> isize;
    fn write(fd: i32, buf: *const u8, count: usize) -> isize;
    fn close(fd: i32) -> i32;
    fn signal(sig: i32, handler: usize) -> usize;
}

#[cfg(unix)]
extern "C" fn cleanup_shared_env() {
    run_process_cleanup();
}

fn run_process_cleanup() {
    if PROCESS_CLEANUP_RUNNING.swap(true, Ordering::SeqCst) {
        return;
    }

    if let Some(env) = SHARED_ENV.get() {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| env.release_process_ref()));
    }
    if let Some(env) = SHARED_SINGLE_NODE_CHURN_ENV.get() {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| env.release_process_ref()));
    }
    if let Some(env) = SHARED_AUTH_ENV.get() {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| env.release_process_ref()));
    }
    if let Some(env) = SHARED_CLUSTER_3_ENV.get() {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| env.release_process_ref()));
    }
    if let Some(env) = SHARED_CLUSTER_3_CHURN_ENV.get() {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| env.release_process_ref()));
    }
    #[cfg(feature = "ssl")]
    if let Some(env) = SHARED_TLS_ENV.get() {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| env.release_process_ref()));
    }
    #[cfg(feature = "ssl")]
    if let Some(env) = SHARED_MTLS_ENV.get() {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| env.release_process_ref()));
    }
}

#[cfg(unix)]
const SIGINT_NUM: i32 = 2;
#[cfg(unix)]
const SIGTERM_NUM: i32 = 15;

#[cfg(unix)]
extern "C" fn cleanup_signal_handler(_sig: i32) {
    let fd = SIGNAL_PIPE_WRITE_FD.load(Ordering::Relaxed);
    if fd < 0 {
        return;
    }

    let byte = [1u8; 1];
    unsafe {
        let _ = write(fd, byte.as_ptr(), byte.len());
    }
}

#[derive(Debug)]
pub struct IgniteTestEnv {
    addr: String,
    username: Option<String>,
    password: Option<String>,
    tls_server_name: Option<String>,
    ca_pem: Option<String>,
    client_cert_pem: Option<String>,
    client_key_pem: Option<String>,
    managed: Option<ManagedIgniteContainer>,
}

#[derive(Debug)]
pub struct IgniteClusterEnv {
    addrs: Vec<String>,
    managed: Option<ManagedIgniteCluster>,
}

#[derive(Debug)]
pub struct DelayedHandshakeEnv {
    addr: String,
    _base_env: Option<Arc<IgniteTestEnv>>,
    join: Option<thread::JoinHandle<()>>,
}

#[derive(Clone, Debug)]
struct ManagedIgniteContainer {
    profile: String,
    name: String,
    port: u16,
    state_root: PathBuf,
    scope: FixtureScope,
}

#[derive(Clone, Debug)]
struct ManagedIgniteCluster {
    profile: String,
    name_prefix: String,
    network_name: String,
    state_root: PathBuf,
    scope: FixtureScope,
}

#[derive(Debug)]
struct SharedContainerState {
    ref_count: usize,
    mapped_port: Option<u16>,
    owner_pids: Vec<u32>,
    bootstrap_version: Option<String>,
}

#[derive(Debug)]
struct FileLockGuard {
    path: PathBuf,
}

impl Default for SharedContainerState {
    fn default() -> Self {
        Self {
            ref_count: 0,
            mapped_port: None,
            owner_pids: Vec::new(),
            bootstrap_version: None,
        }
    }
}

impl Drop for FileLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
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

    fn containerized(profile: &str, scope: FixtureScope) -> Self {
        ensure_docker_backend();
        register_process_cleanup();

        let managed = acquire_managed_container(profile, scope);
        let addr = managed.addr();
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
        Ok(conf)
    }

    pub fn stop(&self) {
        self.managed
            .as_ref()
            .expect("single-node control is only available for managed fixtures")
            .stop();
    }

    pub fn start(&self) {
        self.managed
            .as_ref()
            .expect("single-node control is only available for managed fixtures")
            .start();
    }

    pub fn restart(&self) {
        self.managed
            .as_ref()
            .expect("single-node control is only available for managed fixtures")
            .restart();
    }

    fn release_process_ref(&self) {
        if let Some(managed) = &self.managed {
            managed.release();
        }
    }

    pub async fn wait_for_ready(&self) -> IgniteResult<()> {
        wait_for_client_ready(self.client_config()?).await
    }
}

impl Drop for IgniteTestEnv {
    fn drop(&mut self) {
        self.release_process_ref();
    }
}

impl IgniteClusterEnv {
    fn external(addrs: Vec<String>) -> Self {
        Self {
            addrs,
            managed: None,
        }
    }

    fn containerized(profile: &str, scope: FixtureScope) -> Self {
        ensure_docker_backend();
        register_process_cleanup();

        let managed = acquire_managed_cluster(profile, scope);
        let addrs = managed.addresses();

        Self {
            addrs,
            managed: Some(managed),
        }
    }

    pub fn addr(&self) -> &str {
        self.addrs
            .first()
            .expect("expected at least one cluster fixture address")
    }

    pub fn addresses(&self) -> &[String] {
        &self.addrs
    }

    pub fn node_addr(&self, index: usize) -> String {
        if let Some(managed) = &self.managed {
            return managed.node_address(index);
        }

        self.addrs
            .get(index)
            .cloned()
            .expect("cluster node address index out of bounds for external fixture")
    }

    pub fn is_managed(&self) -> bool {
        self.managed.is_some()
    }

    pub fn stop_node(&self, index: usize) {
        self.managed
            .as_ref()
            .expect("cluster control is only available for managed fixtures")
            .stop_node(index);
    }

    pub fn start_node(&self, index: usize) {
        self.managed
            .as_ref()
            .expect("cluster control is only available for managed fixtures")
            .start_node(index);
    }

    pub fn restart_node(&self, index: usize) {
        self.managed
            .as_ref()
            .expect("cluster control is only available for managed fixtures")
            .restart_node(index);
    }

    pub fn stop_all(&self) {
        self.managed
            .as_ref()
            .expect("cluster control is only available for managed fixtures")
            .stop_all();
    }

    pub fn start_all(&self) {
        self.managed
            .as_ref()
            .expect("cluster control is only available for managed fixtures")
            .start_all();
    }

    pub fn restart_all(&self) {
        self.managed
            .as_ref()
            .expect("cluster control is only available for managed fixtures")
            .restart_all();
    }

    fn stop_extra_nodes(&self) {
        if let Some(managed) = &self.managed {
            managed.stop_extra_nodes();
        }
    }

    fn ensure_base_nodes_running(&self) {
        if let Some(managed) = &self.managed {
            managed.ensure_base_nodes_running();
        }
    }

    async fn restart_base_nodes_sequentially(&self) -> IgniteResult<()> {
        let managed = self
            .managed
            .as_ref()
            .expect("cluster control is only available for managed fixtures");
        managed.stop_all();
        managed.start_node(0);
        wait_for_cluster_ready_with_timeout(&[managed.node_address(0)], CLUSTER_READY_TIMEOUT).await?;
        for index in 1..base_cluster_node_count(&managed.profile) {
            managed.start_node(index);
        }
        Ok(())
    }

    fn release_process_ref(&self) {
        if let Some(managed) = &self.managed {
            managed.release();
        }
    }

    pub async fn wait_for_ready(&self) -> IgniteResult<()> {
        let addrs = if let Some(managed) = &self.managed {
            managed.running_addresses()
        } else {
            self.addrs.clone()
        };

        if addrs.is_empty() {
            return Err(IgniteError::from(
                "cluster fixture has no running nodes to wait for",
            ));
        }

        wait_for_cluster_ready(&addrs).await
    }

    async fn wait_for_ready_with_timeout(&self, timeout: Duration) -> IgniteResult<()> {
        let addrs = if let Some(managed) = &self.managed {
            managed.running_addresses()
        } else {
            self.addrs.clone()
        };

        if addrs.is_empty() {
            return Err(IgniteError::from(
                "cluster fixture has no running nodes to wait for",
            ));
        }

        wait_for_cluster_ready_with_timeout(&addrs, timeout).await
    }
}

impl Drop for IgniteClusterEnv {
    fn drop(&mut self) {
        self.release_process_ref();
    }
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
            IgniteContextKind::Cluster(env) => Ok(ClientConfig::from_addresses(
                env.addresses().iter().cloned(),
            )),
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
        fixture_debug(&format!("ensure_cache({name}): connecting"));
        let client = self.connect_for_bootstrap().await?;
        fixture_debug(&format!("ensure_cache({name}): connected"));
        fixture_debug(&format!("ensure_cache({name}): calling get_or_create_cache"));
        let _ = client.get_or_create_cache::<i32, i32>(name).await?;
        fixture_debug(&format!("ensure_cache({name}): cache ready"));
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
                    fixture_debug("reset_profile_state: restarting managed single-node churn fixture");
                    env.restart();
                }
                fixture_debug("reset_profile_state: waiting for single fixture readiness");
                self.wait_for_ready().await
            }
            IgniteContextKind::Cluster(env) => {
                if self.profile == IgniteProfile::ThreeNodeClusterChurn && env.is_managed() {
                    fixture_debug("reset_profile_state: stopping extra churn nodes");
                    env.stop_extra_nodes();
                    fixture_debug("reset_profile_state: ensuring base churn cluster nodes are running");
                    env.ensure_base_nodes_running();
                    fixture_debug("reset_profile_state: waiting for short churn readiness window");
                    match env.wait_for_ready_with_timeout(CHURN_RESET_READY_TIMEOUT).await {
                        Ok(()) => return Ok(()),
                        Err(err) => {
                            fixture_debug(&format!(
                                "reset_profile_state: short churn readiness failed ({err}); sequentially restarting base churn cluster nodes"
                            ));
                            env.restart_base_nodes_sequentially().await?;
                            fixture_debug(
                                "reset_profile_state: waiting for cluster fixture readiness after restart fallback",
                            );
                            return self.wait_for_ready().await;
                        }
                    }
                }
                fixture_debug("reset_profile_state: waiting for cluster fixture readiness");
                self.wait_for_ready().await
            }
        }
    }

    pub fn stop_node(&self, index: usize) {
        self.cluster_env()
            .expect("cluster node control is only available for cluster fixtures")
            .stop_node(index);
    }

    pub fn start_node(&self, index: usize) {
        self.cluster_env()
            .expect("cluster node control is only available for cluster fixtures")
            .start_node(index);
    }

    pub fn restart_node(&self, index: usize) {
        self.cluster_env()
            .expect("cluster node control is only available for cluster fixtures")
            .restart_node(index);
    }

    pub fn restart_all(&self) {
        self.cluster_env()
            .expect("cluster control is only available for cluster fixtures")
            .restart_all();
    }
}

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

impl ManagedIgniteContainer {
    fn addr(&self) -> String {
        format!("{}:{}", docker_host_addr(), self.port)
    }

    fn stop(&self) {
        stop_container(&self.name);
    }

    fn start(&self) {
        start_container(&self.name);
        ensure_profile_container_ready(&self.name, &self.profile);
    }

    fn restart(&self) {
        restart_container(&self.name);
        ensure_profile_container_ready(&self.name, &self.profile);
    }

    fn release(&self) {
        if self.scope == FixtureScope::Process {
            if container_exists(&self.name) {
                remove_container(&self.name);
            }
            return;
        }

        let lock_path = shared_lock_path(&self.state_root, &self.name);
        let state_path = shared_state_path(&self.state_root, &self.name);
        let _lock = acquire_file_lock(&lock_path);
        let mut state = read_shared_state(&state_path);
        prune_dead_owner_pids(&mut state);

        if state.owner_pids.is_empty() {
            cleanup_shared_container(&self.name, &state_path);
            return;
        }

        if release_owner_pid(&mut state, process::id()) {
            cleanup_shared_container(&self.name, &state_path);
        } else {
            write_shared_state(&state_path, &state);
        }
    }
}

impl ManagedIgniteCluster {
    fn addresses(&self) -> Vec<String> {
        (0..base_cluster_node_count(&self.profile))
            .map(|index| {
                let port = cluster_node_client_port(&self.profile, index);
                format!("{}:{port}", docker_host_addr())
            })
            .collect()
    }

    fn running_addresses(&self) -> Vec<String> {
        (0..managed_cluster_node_count(&self.profile))
            .filter(|index| container_is_running(&self.container_name(*index)))
            .map(|index| {
                let port = cluster_node_client_port(&self.profile, index);
                format!("{}:{port}", docker_host_addr())
            })
            .collect()
    }

    fn node_address(&self, index: usize) -> String {
        assert!(
            index < managed_cluster_node_count(&self.profile),
            "cluster node index {} out of bounds for profile {}",
            index,
            self.profile
        );
        let port = cluster_node_client_port(&self.profile, index);
        format!("{}:{port}", docker_host_addr())
    }

    fn container_name(&self, index: usize) -> String {
        format!("{}-node-{}", self.name_prefix, index)
    }

    fn stop_node(&self, index: usize) {
        assert_cluster_node_index(&self.profile, index);
        stop_container(&self.container_name(index));
    }

    fn start_node(&self, index: usize) {
        assert_cluster_node_index(&self.profile, index);
        ensure_cluster_node_running(
            &self.profile,
            &self.name_prefix,
            &self.network_name,
            index,
        );
    }

    fn restart_node(&self, index: usize) {
        assert_cluster_node_index(&self.profile, index);
        let name = self.container_name(index);
        if container_exists(&name) {
            stop_container(&name);
            ensure_cluster_node_running(
                &self.profile,
                &self.name_prefix,
                &self.network_name,
                index,
            );
        } else {
            ensure_cluster_node_running(
                &self.profile,
                &self.name_prefix,
                &self.network_name,
                index,
            );
        }
    }

    fn stop_all(&self) {
        for index in 0..base_cluster_node_count(&self.profile) {
            self.stop_node(index);
        }
    }

    fn start_all(&self) {
        for index in 0..base_cluster_node_count(&self.profile) {
            self.start_node(index);
        }
    }

    fn restart_all(&self) {
        for index in 0..base_cluster_node_count(&self.profile) {
            self.restart_node(index);
        }
    }

    fn ensure_base_nodes_running(&self) {
        for index in 0..base_cluster_node_count(&self.profile) {
            ensure_cluster_node_running(&self.profile, &self.name_prefix, &self.network_name, index);
        }
    }

    fn stop_extra_nodes(&self) {
        for index in base_cluster_node_count(&self.profile)..managed_cluster_node_count(&self.profile)
        {
            let name = self.container_name(index);
            if container_exists(&name) && container_is_running(&name) {
                stop_container(&name);
            }
        }
    }

    fn release(&self) {
        if self.scope == FixtureScope::Process {
            cleanup_process_cluster(self);
            return;
        }

        let lock_path = shared_lock_path(&self.state_root, &self.profile);
        let state_path = shared_state_path(&self.state_root, &self.profile);
        let _lock = acquire_file_lock(&lock_path);
        let mut state = read_shared_state(&state_path);
        prune_dead_owner_pids(&mut state);

        if state.owner_pids.is_empty() {
            cleanup_shared_cluster(self);
            return;
        }

        if release_owner_pid(&mut state, process::id()) {
            cleanup_shared_cluster(self);
        } else {
            write_shared_state(&state_path, &state);
        }
    }
}

fn register_process_cleanup() {
    PROCESS_CLEANUP_READY.get_or_init(|| {
        #[cfg(unix)]
        unsafe {
            let rc = atexit(cleanup_shared_env);
            assert_eq!(rc, 0, "failed to register Ignite test env cleanup");
        }

        #[cfg(unix)]
        register_signal_cleanup();
    });
}

#[cfg(unix)]
fn register_signal_cleanup() {
    SIGNAL_CLEANUP_READY.get_or_init(|| {
        let mut fds = [-1i32; 2];
        let rc = unsafe { pipe(fds.as_mut_ptr()) };
        if rc != 0 {
            return;
        }

        let read_fd = fds[0];
        let write_fd = fds[1];
        SIGNAL_PIPE_WRITE_FD.store(write_fd, Ordering::SeqCst);

        unsafe {
            let _ = signal(SIGINT_NUM, cleanup_signal_handler as usize);
            let _ = signal(SIGTERM_NUM, cleanup_signal_handler as usize);
        }

        thread::spawn(move || {
            let mut buf = [0u8; 1];
            let rc = unsafe { read(read_fd, buf.as_mut_ptr(), buf.len()) };
            if rc > 0 {
                run_process_cleanup();
            }

            unsafe {
                let _ = close(read_fd);
                let _ = close(write_fd);
            }
            SIGNAL_PIPE_WRITE_FD.store(-1, Ordering::SeqCst);
        });
    });
}

fn cleanup_shared_container(name: &str, state_path: &Path) {
    if container_exists(name) {
        remove_container(name);
    }

    let _ = fs::remove_file(state_path);
}

fn cleanup_shared_cluster(cluster: &ManagedIgniteCluster) {
    for index in 0..managed_cluster_node_count(&cluster.profile) {
        let container_name = cluster.container_name(index);
        if container_exists(&container_name) {
            remove_container(&container_name);
        }
    }

    if network_exists(&cluster.network_name) {
        remove_network(&cluster.network_name);
    }

    let _ = fs::remove_file(shared_state_path(&cluster.state_root, &cluster.profile));
    let _ = fs::remove_dir_all(cluster_generated_config_root(
        &cluster.profile,
        &cluster.name_prefix,
    ));
}

fn cleanup_process_cluster(cluster: &ManagedIgniteCluster) {
    for index in 0..managed_cluster_node_count(&cluster.profile) {
        let container_name = cluster.container_name(index);
        if container_exists(&container_name) {
            remove_container(&container_name);
        }
    }

    if network_exists(&cluster.network_name) {
        remove_network(&cluster.network_name);
    }

    let _ = fs::remove_dir_all(cluster_generated_config_root(
        &cluster.profile,
        &cluster.name_prefix,
    ));
}

fn acquire_managed_container(profile: &str, scope: FixtureScope) -> ManagedIgniteContainer {
    match scope {
        FixtureScope::Process => acquire_process_container(profile),
        FixtureScope::CargoSession => acquire_shared_container(profile),
    }
}

fn acquire_managed_cluster(profile: &str, scope: FixtureScope) -> ManagedIgniteCluster {
    match scope {
        FixtureScope::Process => acquire_process_cluster(profile),
        FixtureScope::CargoSession => acquire_shared_cluster(profile),
    }
}

fn acquire_shared_container(profile: &str) -> ManagedIgniteContainer {
    let state_root = shared_state_root();
    fs::create_dir_all(&state_root).expect("failed to create shared Ignite fixture dir");

    let name = test_container_name_for(profile);
    let lock_path = shared_lock_path(&state_root, &name);
    let state_path = shared_state_path(&state_root, &name);
    let _lock = acquire_file_lock(&lock_path);

    let recreated = ensure_shared_container_running(&name, profile);

    let mut state = read_shared_state(&state_path);
    prune_dead_owner_pids(&mut state);
    if recreated || state.mapped_port.is_none() {
        state.mapped_port = Some(mapped_host_port(&name));
    }
    state.bootstrap_version = Some(shared_bootstrap_version(profile));
    if !state.owner_pids.contains(&process::id()) {
        state.owner_pids.push(process::id());
    }
    state.ref_count = state.owner_pids.len();
    write_shared_state(&state_path, &state);

    ManagedIgniteContainer {
        profile: profile.to_string(),
        name,
        port: state
            .mapped_port
            .expect("managed Ignite fixture missing mapped thin-client port"),
        state_root,
        scope: FixtureScope::CargoSession,
    }
}

fn acquire_shared_cluster(profile: &str) -> ManagedIgniteCluster {
    let state_root = shared_state_root();
    fs::create_dir_all(&state_root).expect("failed to create shared Ignite fixture dir");

    let profile = profile.to_string();
    let name_prefix = managed_cluster_name_prefix(&profile, FixtureScope::CargoSession);
    let network_name = format!("{name_prefix}-net");
    let lock_path = shared_lock_path(&state_root, &profile);
    let state_path = shared_state_path(&state_root, &profile);
    let _lock = acquire_file_lock(&lock_path);

    ensure_shared_cluster_running(profile.as_str(), &name_prefix, &network_name);

    let mut state = read_shared_state(&state_path);
    prune_dead_owner_pids(&mut state);
    state.bootstrap_version = Some(shared_bootstrap_version(profile.as_str()));
    if !state.owner_pids.contains(&process::id()) {
        state.owner_pids.push(process::id());
    }
    state.ref_count = state.owner_pids.len();
    write_shared_state(&state_path, &state);

    ManagedIgniteCluster {
        profile,
        name_prefix,
        network_name,
        state_root,
        scope: FixtureScope::CargoSession,
    }
}

fn acquire_process_container(profile: &str) -> ManagedIgniteContainer {
    let state_root = shared_state_root();
    fs::create_dir_all(&state_root).expect("failed to create process Ignite fixture dir");

    let name = process_container_name_for(profile);
    ensure_shared_container_running(&name, profile);
    let port = mapped_host_port(&name);

    ManagedIgniteContainer {
        profile: profile.to_string(),
        name,
        port,
        state_root,
        scope: FixtureScope::Process,
    }
}

fn acquire_process_cluster(profile: &str) -> ManagedIgniteCluster {
    let state_root = shared_state_root();
    fs::create_dir_all(&state_root).expect("failed to create process Ignite fixture dir");

    let name_prefix = managed_cluster_name_prefix(profile, FixtureScope::Process);
    let network_name = format!("{name_prefix}-net");

    ensure_shared_cluster_running(profile, &name_prefix, &network_name);

    ManagedIgniteCluster {
        profile: profile.to_string(),
        name_prefix,
        network_name,
        state_root,
        scope: FixtureScope::Process,
    }
}

fn ensure_shared_container_running(name: &str, profile: &str) -> bool {
    let expected_config_path = single_node_container_config_path();
    let mut recreated = false;

    if container_exists(name)
        && !container_matches_expected_config(name, expected_config_path, profile, None, None, None)
    {
        remove_container(name);
        recreated = true;
    }

    if !container_exists(name) {
        start_new_container(name, profile);
        assert!(
            container_matches_expected_config(name, expected_config_path, profile, None, None, None),
            "managed Ignite fixture container {} started without expected config {}",
            name,
            expected_config_path
        );
        ensure_profile_container_ready(name, profile);
        return true;
    }

    if !container_is_running(name) {
        start_container(name);
    }

    ensure_profile_container_ready(name, profile);
    recreated
}

fn ensure_shared_cluster_running(profile: &str, name_prefix: &str, network_name: &str) {
    fixture_debug(&format!(
        "ensure_shared_cluster_running: profile={profile} network={network_name}"
    ));
    ensure_network_exists(network_name);

    for index in base_cluster_node_count(profile)..managed_cluster_node_count(profile) {
        let container_name = format!("{name_prefix}-node-{index}");
        if container_exists(&container_name) {
            fixture_debug(&format!(
                "ensure_shared_cluster_running: removing extra node container {container_name}"
            ));
            remove_container(&container_name);
        }
    }

    for index in 0..base_cluster_node_count(profile) {
        ensure_cluster_node_running(profile, name_prefix, network_name, index);
    }
}

fn ensure_cluster_node_running(
    profile: &str,
    name_prefix: &str,
    network_name: &str,
    index: usize,
) {
    let container_name = cluster_node_container_name(name_prefix, index);
    let discovery_seed_addresses =
        cluster_discovery_seed_addresses(profile, name_prefix, network_name, index);
    let config_path = generated_cluster_config_path(
        profile,
        name_prefix,
        network_name,
        index,
        &discovery_seed_addresses,
    );
    let expected_config_path = cluster_node_container_config_path();
    let expected_client_port = cluster_node_client_port(profile, index);
    let expected_network_ip = container_network_ip(&container_name, network_name)
        .or_else(|| cluster_node_network_ip(network_name, index));
    fixture_debug(&format!(
        "ensure_cluster_node_running: profile={profile} node={index} name={container_name} discovery_seeds={:?}",
        discovery_seed_addresses
    ));

    if container_exists(&container_name)
        && !container_matches_expected_config(
            &container_name,
            expected_config_path,
            profile,
            Some(network_name),
            expected_network_ip.as_deref(),
            Some(expected_client_port),
        )
    {
        remove_container(&container_name);
    }

    if container_exists(&container_name)
        && !container_is_running(&container_name)
        && stopped_container_requires_recreate(&container_name)
    {
        fixture_debug(&format!(
            "ensure_cluster_node_running: recreating failed stopped cluster node container {container_name}"
        ));
        remove_container(&container_name);
    }

    if !container_exists(&container_name) {
        fixture_debug(&format!(
            "ensure_cluster_node_running: creating cluster node container {container_name}"
        ));
        start_new_cluster_node(profile, &container_name, network_name, &config_path);
        ensure_cluster_node_runtime_config(
            profile,
            name_prefix,
            network_name,
            index,
            &container_name,
        );
        start_container(&container_name);
        let expected_network_ip = container_network_ip(&container_name, network_name)
            .or_else(|| cluster_node_network_ip(network_name, index));
        assert!(
            container_matches_expected_config(
                &container_name,
                expected_config_path,
                profile,
                Some(network_name),
                expected_network_ip.as_deref(),
                Some(expected_client_port),
            ),
            "managed Ignite cluster container {} started without expected config {}",
            container_name,
            expected_config_path
        );
        return;
    }

    if !container_is_running(&container_name) {
        fixture_debug(&format!(
            "ensure_cluster_node_running: starting existing cluster node container {container_name}"
        ));
        start_container(&container_name);
    }

    ensure_cluster_node_runtime_config(
        profile,
        name_prefix,
        network_name,
        index,
        &container_name,
    );
}

fn start_new_container(name: &str, profile: &str) {
    let config_path = fixture_config_path(profile, 0);
    let image = ignite_runnable_image(
        profile,
        name,
        None,
        &config_path,
        single_node_container_config_path(),
    );

    create_managed_container(profile, image, true);
}

fn ensure_profile_container_ready(name: &str, profile: &str) {
    if profile == SINGLE_NODE_AUTH_PROFILE {
        ensure_cluster_active(name);
    }
}

fn ensure_cluster_active(name: &str) {
    let addr = format!("{}:{}", docker_host_addr(), mapped_host_port(name));
    let mut conf = ClientConfig::new(&addr);
    conf.username = Some(DEFAULT_AUTH_USERNAME.to_string());
    conf.password = Some(DEFAULT_AUTH_PASSWORD.to_string());

    block_on_fixture(async move {
        let mut last_err = None;

        for _ in 0..start_retries() {
            match new_client(conf.clone()).await {
                Ok(client) => match client.cluster().state().await {
                    Ok(ClusterState::Active) => return,
                    Ok(_) => {
                        if let Err(err) = client.cluster().set_state(ClusterState::Active).await {
                            last_err = Some(err);
                        } else {
                            return;
                        }
                    }
                    Err(err) => last_err = Some(err),
                },
                Err(err) => last_err = Some(err),
            }

            tokio::time::sleep(start_delay()).await;
        }

        panic!(
            "failed to activate Ignite auth fixture {}: {}",
            name,
            last_err
                .map(|err| err.to_string())
                .unwrap_or_else(|| "unknown activation error".to_string())
        );
    });
}

fn start_new_cluster_node(
    profile: &str,
    container_name: &str,
    network_name: &str,
    config_path: &Path,
) {
    fixture_debug(&format!(
        "start_new_cluster_node: profile={profile} container={container_name}"
    ));
    let image = ignite_runnable_image(
        profile,
        container_name,
        Some(network_name),
        config_path,
        cluster_node_container_config_path(),
    );

    create_managed_container(profile, image, false);
}

fn container_exists(name: &str) -> bool {
    block_on_fixture(async move {
        match tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker_client().inspect_container(name, None),
        )
        .await
        {
            Ok(result) => result.is_ok(),
            Err(_) => container_exists_by_listing(name).await,
        }
    })
}

fn container_is_running(name: &str) -> bool {
    block_on_fixture(async move {
        match tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker_client().inspect_container(name, None),
        )
        .await
        {
            Ok(result) => result
                .ok()
                .and_then(|inspect| inspect.state)
                .and_then(|state| state.running)
                .unwrap_or(false),
            Err(_) => container_running_by_listing(name).await,
        }
    })
}

fn stopped_container_requires_recreate(name: &str) -> bool {
    block_on_fixture(async move {
        let inspect = match tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker_client().inspect_container(name, None),
        )
        .await
        {
            Ok(Ok(inspect)) => inspect,
            _ => return false,
        };

        inspect
            .state
            .as_ref()
            .map(|state| {
                let has_runtime_error = state
                    .error
                    .as_deref()
                    .map(|error| !error.trim().is_empty())
                    .unwrap_or(false);
                let exit_code = state.exit_code.unwrap_or_default();
                let oom_killed = state.oom_killed.unwrap_or(false);

                oom_killed || has_runtime_error || !matches!(exit_code, 0 | 137 | 143)
            })
            .unwrap_or(false)
    })
}

async fn container_exists_by_listing(name: &str) -> bool {
    let filters = HashMap::from([("name".to_string(), vec![name.to_string()])]);
    tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker_client().list_containers(Some(ListContainersOptions::<String> {
            all: true,
            filters,
            ..Default::default()
        })),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .map(|containers| {
        containers.into_iter().any(|container| {
            container
                .names
                .unwrap_or_default()
                .into_iter()
                .any(|candidate| candidate.trim_start_matches('/') == name)
        })
    })
    .unwrap_or(false)
}

async fn container_running_by_listing(name: &str) -> bool {
    let filters = HashMap::from([("name".to_string(), vec![name.to_string()])]);
    tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker_client().list_containers(Some(ListContainersOptions::<String> {
            all: true,
            filters,
            ..Default::default()
        })),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .map(|containers| {
        containers.into_iter().any(|container| {
            let matches_name = container
                .names
                .unwrap_or_default()
                .into_iter()
                .any(|candidate| candidate.trim_start_matches('/') == name);
            matches_name && container.state.as_deref() == Some("running")
        })
    })
    .unwrap_or(false)
}

fn container_matches_expected_config(
    name: &str,
    container_config_path: &str,
    profile: &str,
    expected_network: Option<&str>,
    expected_network_ip: Option<&str>,
    expected_host_port: Option<u16>,
) -> bool {
    block_on_fixture(async move {
        let inspect = match tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker_client().inspect_container(name, None),
        )
        .await
        {
            Ok(Ok(inspect)) => inspect,
            _ => return false,
        };

        let expected_env = format!("CONFIG_URI={container_config_path}");
        let expected_jvm_opts = format!("JVM_OPTS={}", profile_jvm_opts(profile));
        let env_matches = inspect
            .config
            .as_ref()
            .and_then(|config| config.env.as_ref())
            .map(|env| {
                env.iter().any(|entry| entry == &expected_env)
                    && env.iter().any(|entry| entry == &expected_jvm_opts)
            })
            .unwrap_or(false);

        let bind_matches = inspect
            .host_config
            .as_ref()
            .and_then(|host_config| host_config.binds.as_ref())
            .map(|binds| {
                binds.iter().any(|bind| {
                    bind.split(':')
                        .nth(1)
                        .map(|dest| dest == container_config_path)
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);

        let mount_matches = inspect
            .mounts
            .as_ref()
            .map(|mounts| {
                mounts.iter().any(|mount| {
                    mount
                        .destination
                        .as_deref()
                        .map(|dest| dest == container_config_path)
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);

        let network_matches = match expected_network {
            Some(expected_network) => inspect
                .network_settings
                .as_ref()
                .and_then(|network_settings| network_settings.networks.as_ref())
                .and_then(|networks| networks.get(expected_network))
                .map(|_endpoint| true)
                .or_else(|| {
                    inspect
                        .host_config
                        .as_ref()
                        .and_then(|host_config| host_config.network_mode.as_deref())
                        .map(|network_mode| network_mode == expected_network)
                })
                .unwrap_or(false),
            None => inspect
                .host_config
                .as_ref()
                .and_then(|host_config| host_config.network_mode.as_deref())
                .map(|network_mode| {
                    network_mode.is_empty()
                        || network_mode == "default"
                        || network_mode == "bridge"
                })
                .unwrap_or(true),
        };

        let network_ip_matches = match (expected_network, expected_network_ip) {
            (Some(expected_network), Some(expected_network_ip)) => inspect
                .network_settings
                .as_ref()
                .and_then(|network_settings| network_settings.networks.as_ref())
                .and_then(|networks| networks.get(expected_network))
                .and_then(endpoint_ip_address)
                .map(|ip| ip == expected_network_ip)
                .unwrap_or(false),
            _ => true,
        };

        let port_matches = match expected_host_port {
            Some(expected_host_port) => inspect
                .host_config
                .as_ref()
                .and_then(|host_config| host_config.port_bindings.as_ref())
                .and_then(|bindings| bindings.get(&format!("{expected_host_port}/tcp")))
                .and_then(|bindings| bindings.as_ref())
                .and_then(|bindings| bindings.first())
                .and_then(|binding| binding.host_port.as_deref())
                .and_then(|host_port| host_port.parse::<u16>().ok())
                .map(|host_port| host_port == expected_host_port)
                .unwrap_or(false),
            None => true,
        };

        env_matches
            && (bind_matches || mount_matches)
            && network_matches
            && network_ip_matches
            && port_matches
    })
}

fn wait_for_container_running_state(name: &str, expected: bool, timeout: Duration) {
    let started = Instant::now();

    loop {
        if container_is_running(name) == expected {
            return;
        }

        if started.elapsed() >= timeout {
            panic!(
                "timed out waiting for Ignite test container {} running={} after {:?}",
                name, expected, timeout
            );
        }

        thread::sleep(CONTAINER_STATE_POLL_DELAY);
    }
}

fn start_container(name: &str) {
    fixture_debug(&format!("start_container: {name}"));
    block_on_fixture(async move {
        tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker_client().start_container::<String>(name, None::<StartContainerOptions<String>>),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "timed out starting existing Ignite test container {} after {:?}",
                name, DOCKER_API_TIMEOUT
            )
        })
        .unwrap_or_else(|err| {
            let msg = err.to_string();
            if msg.contains("is already started") {
                return;
            }
            panic!(
                "failed to start existing Ignite test container {}: {}",
                name, err
            )
        });
    });

    wait_for_container_running_state(name, true, CONTAINER_START_TIMEOUT);
}

fn stop_container(name: &str) {
    fixture_debug(&format!("stop_container: {name}"));
    block_on_fixture(async move {
        tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker_client().stop_container(name, Some(StopContainerOptions { t: 1 })),
        )
        .await
        .unwrap_or_else(|_| {
            panic!(
                "timed out stopping Ignite test container {} after {:?}",
                name, DOCKER_API_TIMEOUT
            )
        })
        .unwrap_or_else(|err| {
            let msg = err.to_string();
            if msg.contains("is not running") || msg.contains("No such container") {
                return;
            }
            panic!("failed to stop Ignite test container {}: {}", name, err)
        });
    });

    wait_for_container_running_state(name, false, CONTAINER_STOP_TIMEOUT);
}

fn restart_container(name: &str) {
    stop_container(name);
    start_container(name);
}

fn remove_container(name: &str) {
    fixture_debug(&format!("remove_container: {name}"));
    block_on_fixture(async move {
        tokio::time::timeout(
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
        .await
        .unwrap_or_else(|_| {
            panic!(
                "timed out removing Ignite test container {} after {:?}",
                name, DOCKER_API_TIMEOUT
            )
        })
        .unwrap_or_else(|err| panic!("failed to remove Ignite test container {}: {}", name, err));
    });
}

fn ensure_network_exists(name: &str) {
    if network_exists(name) {
        fixture_debug(&format!("ensure_network_exists: reusing network {name}"));
        return;
    }

    fixture_debug(&format!("ensure_network_exists: creating network {name}"));

    let created = block_on_fixture(async move {
        match tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker_client().create_network(CreateNetworkOptions {
                name: name.to_string(),
                check_duplicate: true,
                labels: managed_resource_labels_from_name(name, "network"),
                ..Default::default()
            }),
        )
        .await
        {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(err)) => {
                let msg = err.to_string();
                if msg.contains("already exists") || msg.contains("exists") {
                    Ok(())
                } else {
                    Err(msg)
                }
            }
            Err(_) => Err(format!(
                "timed out creating Ignite test network {} after {:?}",
                name, DOCKER_API_TIMEOUT
            )),
        }
    });

    match created {
        Ok(()) => return,
        Err(_err) if network_exists(name) => return,
        Err(_err) if wait_for_network_exists(name, NETWORK_CREATE_SETTLE_TIMEOUT) => return,
        Err(err) => panic!("failed to ensure Ignite test network {}: {}", name, err),
    }
}

fn network_exists(name: &str) -> bool {
    block_on_fixture(async move { network_exists_by_listing(name).await })
}

fn remove_network(name: &str) {
    block_on_fixture(async move {
        tokio::time::timeout(DOCKER_API_TIMEOUT, docker_client().remove_network(name))
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "timed out removing Ignite test network {} after {:?}",
                    name, DOCKER_API_TIMEOUT
                )
            })
            .unwrap_or_else(|err| {
                let msg = err.to_string();
                if msg.contains("No such network") || msg.contains("not found") {
                    return;
                }
                panic!("failed to remove Ignite test network {}: {}", name, err)
            });
    });
}

async fn network_exists_by_listing(name: &str) -> bool {
    tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker_client().list_networks::<String>(None),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .map(|networks| {
        networks.into_iter().any(|network| {
            network
                .name
                .as_deref()
                .map(|candidate| candidate == name)
                .unwrap_or(false)
        })
    })
    .unwrap_or(false)
}

fn wait_for_network_exists(name: &str, timeout: Duration) -> bool {
    let started = Instant::now();

    loop {
        if network_exists(name) {
            return true;
        }

        if started.elapsed() >= timeout {
            return false;
        }

        thread::sleep(CONTAINER_STATE_POLL_DELAY);
    }
}

fn mapped_host_port(name: &str) -> u16 {
    let port_spec = format!("{IGNITE_PORT}/tcp");
    for _ in 0..start_retries() {
        let port_spec = port_spec.clone();
        if let Some(port) =
            block_on_fixture(async move { mapped_host_port_once(name, &port_spec).await })
        {
            return port;
        }

        thread::sleep(start_delay());
    }

    panic!(
        "failed to inspect mapped Ignite thin-client port for {} after {} retries",
        name,
        start_retries()
    );
}

fn cluster_node_client_port(profile: &str, index: usize) -> u16 {
    assert!(index <= u16::MAX as usize, "cluster node index exceeded u16");
    let offset = index as u16;

    match profile {
        CLUSTER_3_PROFILE => CLUSTER_3_BASE_PORT + offset,
        CLUSTER_3_CHURN_PROFILE => CLUSTER_3_CHURN_BASE_PORT + offset,
        _ => IGNITE_PORT + offset,
    }
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
        "cluster node index {} out of bounds for profile {}",
        index,
        profile
    );
}

fn managed_cluster_name_prefix(profile: &str, scope: FixtureScope) -> String {
    match scope {
        FixtureScope::CargoSession => format!(
            "ignite-rs-{}",
            sanitize_identifier(&format!(
                "{profile}-{}-layout-{CLUSTER_LAYOUT_VERSION}",
                test_image_ref()
            ))
        ),
        FixtureScope::Process => format!(
            "ignite-rs-proc-{}-layout-{CLUSTER_LAYOUT_VERSION}-{}",
            sanitize_identifier(profile),
            process::id()
        ),
    }
}

async fn mapped_host_port_once(name: &str, port_spec: &str) -> Option<u16> {
    if let Ok(Ok(inspect)) = tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker_client().inspect_container(name, None),
    )
    .await
    {
        if let Some(port) = inspect
            .network_settings
            .and_then(|settings| settings.ports)
            .and_then(|mut ports| ports.remove(port_spec))
            .and_then(|bindings| bindings)
            .and_then(|bindings| bindings.into_iter().find_map(|binding| binding.host_port))
            .and_then(|host_port| host_port.parse::<u16>().ok())
        {
            return Some(port);
        }
    }

    mapped_host_port_by_listing(name).await
}

fn container_bridge_ip(name: &str) -> Option<String> {
    block_on_fixture(async move {
        match tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker_client().inspect_container(name, None),
        )
        .await
        {
            Ok(Ok(inspect)) => inspect
                .network_settings
                .and_then(|settings| settings.networks)
                .and_then(|networks| {
                    networks
                        .get("bridge")
                        .and_then(endpoint_ip_address)
                        .or_else(|| networks.values().find_map(endpoint_ip_address))
                }),
            _ => None,
        }
    })
}

fn endpoint_ip_address(
    endpoint: &bollard::models::EndpointSettings,
) -> Option<String> {
    endpoint
        .ip_address
        .as_ref()
        .filter(|ip| !ip.trim().is_empty())
        .cloned()
}

fn container_network_ip(container_name: &str, network_name: &str) -> Option<String> {
    block_on_fixture(async move {
        match tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker_client().inspect_container(container_name, None),
        )
        .await
        {
            Ok(Ok(inspect)) => inspect
                .network_settings
                .and_then(|network_settings| network_settings.networks)
                .and_then(|networks| networks.get(network_name).and_then(endpoint_ip_address)),
            _ => None,
        }
    })
}

async fn mapped_host_port_by_listing(name: &str) -> Option<u16> {
    let filters = HashMap::from([("name".to_string(), vec![name.to_string()])]);
    tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker_client().list_containers(Some(ListContainersOptions::<String> {
            all: true,
            filters,
            ..Default::default()
        })),
    )
    .await
    .ok()
    .and_then(Result::ok)
    .and_then(|containers| {
        containers.into_iter().find_map(|container| {
            let matches_name = container
                .names
                .unwrap_or_default()
                .into_iter()
                .any(|candidate| candidate.trim_start_matches('/') == name);
            if !matches_name {
                return None;
            }

            container
                .ports
                .unwrap_or_default()
                .into_iter()
                .find_map(|port| {
                    (port.private_port == i64::from(IGNITE_PORT))
                        .then_some(port.public_port)
                        .flatten()
                        .map(|port| port as u16)
                })
        })
    })
}

fn ensure_docker_backend() {
    let _ = docker_backend();
}

fn docker_backend() -> &'static ManagedDockerBackend {
    DOCKER_BACKEND.get_or_init(|| {
        let docker = if env::var_os("DOCKER_HOST").is_some() {
            Docker::connect_with_http_defaults()
        } else {
            Docker::connect_with_local_defaults().or_else(|_| Docker::connect_with_http_defaults())
        }
        .unwrap_or_else(|err| {
            panic!(
                "tests require a reachable Docker-compatible API endpoint via DOCKER_HOST or the default local socket, or external fixture env vars such as IGNITE_ADDR / IGNITE_3NODE_ADDRS / IGNITE_AUTH_ADDR / IGNITE_TLS_ADDR: {}",
                err
            )
        });

        ManagedDockerBackend { docker }
    })
}

fn docker_client() -> Docker {
    docker_backend().docker.clone()
}

fn docker_host_addr() -> String {
    env::var("TESTCONTAINERS_HOST_OVERRIDE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

fn block_on_fixture<F>(future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    if TokioHandle::try_current().is_ok() {
        thread::scope(|scope| {
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
        })
    } else {
        TokioRuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build fixture runtime")
            .block_on(future)
    }
}

fn ignite_runnable_image(
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
    if let Some(network_name) = network_name {
        runnable = runnable.with_network(network_name.to_string());
    }

    if matches!(profile, CLUSTER_3_PROFILE | CLUSTER_3_CHURN_PROFILE) {
        let index = cluster_node_index(container_name)
            .expect("cluster fixture container name missing node index");
        let port = cluster_node_client_port(profile, index);
        runnable = runnable.with_mapped_port((port, port));
    }

    runnable
}

fn cluster_node_index(container_name: &str) -> Option<usize> {
    container_name
        .rsplit_once("-node-")
        .and_then(|(_, suffix)| suffix.parse::<usize>().ok())
}

fn cluster_node_container_name(name_prefix: &str, index: usize) -> String {
    format!("{name_prefix}-node-{index}")
}

fn profile_jvm_opts(profile: &str) -> &'static str {
    match profile {
        CLUSTER_3_PROFILE | CLUSTER_3_CHURN_PROFILE => "-Xms256m -Xmx256m -DIGNITE_QUIET=false",
        _ => "-Xms512m -Xmx512m -DIGNITE_QUIET=false",
    }
}

fn create_managed_container(
    profile: &str,
    image: RunnableImage<GenericImage>,
    start_after_create: bool,
) {
    block_on_fixture(async move {
        let envs = image
            .env_vars()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>();
        let binds = image
            .volumes()
            .map(|(host_path, container_path)| format!("{host_path}:{container_path}:ro"))
            .collect::<Vec<_>>();
        let mut exposed_ports = image
            .expose_ports()
            .into_iter()
            .map(|port| (format!("{port}/tcp"), HashMap::new()))
            .collect::<HashMap<_, _>>();
        let explicit_ports = image.ports().clone();
        if let Some(ports) = &explicit_ports {
            for port in ports {
                exposed_ports.insert(format!("{}/tcp", port.internal), HashMap::new());
            }
        }
        let args = image
            .args()
            .clone()
            .into_iterator()
            .collect::<Vec<String>>();

        let port_bindings = explicit_ports.as_ref().map(|ports| {
            let mut bindings = HashMap::with_capacity(ports.len());
            for port in ports {
                bindings.insert(
                    format!("{}/tcp", port.internal),
                    Some(vec![PortBinding {
                        host_ip: Some("127.0.0.1".to_string()),
                        host_port: Some(port.local.to_string()),
                    }]),
                );
            }
            bindings
        });

        let container_name = image
            .container_name()
            .clone()
            .expect("fixture image missing container name");
        let network_name = image.network().clone();
        let networking_config = network_name.as_ref().map(|network_name| {
            let endpoint_config = cluster_node_index(&container_name)
                .and_then(|index| cluster_node_network_ip(network_name, index))
                .map(|ip| EndpointSettings {
                    ipam_config: Some(EndpointIpamConfig {
                        ipv4_address: Some(ip),
                        ..Default::default()
                    }),
                    aliases: Some(vec![container_name.clone()]),
                    ..Default::default()
                })
                .unwrap_or_else(|| EndpointSettings {
                    aliases: Some(vec![container_name.clone()]),
                    ..Default::default()
                });

            NetworkingConfig {
                endpoints_config: HashMap::from([(network_name.clone(), endpoint_config)]),
            }
        });

        let config = DockerContainerConfig {
            image: Some(image.descriptor()),
            env: Some(envs),
            labels: Some(managed_resource_labels(profile, "container")),
            host_config: Some(HostConfig {
                binds: if binds.is_empty() { None } else { Some(binds) },
                network_mode: image.network().clone(),
                port_bindings,
                publish_all_ports: Some(explicit_ports.is_none()),
                ..Default::default()
            }),
            networking_config,
            entrypoint: image.entrypoint().map(|entrypoint| vec![entrypoint]),
            cmd: if args.is_empty() { None } else { Some(args) },
            exposed_ports: Some(exposed_ports),
            ..Default::default()
        };
        fixture_debug(&format!(
            "create_managed_container: profile={profile} container={container_name} network={:?}",
            network_name
        ));

        let create = |config| async {
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
        };

        match create(config.clone()).await {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => {
                let msg = err.to_string();
                if msg.contains("No such image") || msg.contains("not found") {
                    ensure_image_available().await;
                    if let Err(err) = create(config)
                        .await
                        .expect("timed out creating fixture container after pulling image")
                    {
                        let msg = err.to_string();
                        if !msg.contains("name is already in use") {
                            panic!("failed to create Ignite fixture container: {}", msg);
                        }
                    }
                } else if !msg.contains("name is already in use") {
                    panic!("failed to create Ignite fixture container: {}", msg);
                }
            }
            Err(_) => {
                if container_exists_by_listing(&container_name).await {
                    // The Docker-compatible API may time out on create while a stopped named container
                    // already exists. Reuse that container instead of treating the timeout as fatal.
                } else {
                    panic!(
                        "timed out creating Ignite fixture container {} after {:?}",
                        container_name, DOCKER_API_TIMEOUT
                    )
                }
            }
        }

        if let Some(network_name) = network_name {
            connect_container_to_network(&container_name, &network_name).await;
        }

        if start_after_create {
            tokio::time::timeout(
                DOCKER_API_TIMEOUT,
                docker_client()
                    .start_container::<String>(&container_name, None::<StartContainerOptions<String>>),
            )
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "timed out starting Ignite fixture container {} after {:?}",
                    container_name, DOCKER_API_TIMEOUT
                )
            })
            .unwrap_or_else(|err| {
                let msg = err.to_string();
                if msg.contains("is already started") {
                    return;
                }
                panic!("failed to start Ignite fixture container: {}", msg)
            });
        }
    });
}

async fn connect_container_to_network(container_name: &str, network_name: &str) {
    fixture_debug(&format!(
        "connect_container_to_network: container={container_name} network={network_name}"
    ));
    let endpoint_config = cluster_node_index(container_name)
        .and_then(|index| cluster_node_network_ip(network_name, index))
        .map(|ip| EndpointSettings {
            ipam_config: Some(EndpointIpamConfig {
                ipv4_address: Some(ip),
                ..Default::default()
            }),
            aliases: Some(vec![container_name.to_string()]),
            ..Default::default()
        })
        .unwrap_or_else(|| EndpointSettings {
            aliases: Some(vec![container_name.to_string()]),
            ..Default::default()
        });

    match tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker_client().connect_network(
            network_name,
            ConnectNetworkOptions {
                container: container_name.to_string(),
                endpoint_config,
            },
        ),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            let msg = err.to_string();
            if msg.contains("already exists")
                || msg.contains("already connected")
                || msg.contains("endpoint with name")
            {
                return;
            }
            panic!(
                "failed to connect Ignite fixture container {} to network {}: {}",
                container_name, network_name, msg
            );
        }
        Err(_) => panic!(
            "timed out connecting Ignite fixture container {} to network {} after {:?}",
            container_name, network_name, DOCKER_API_TIMEOUT
        ),
    }
}

async fn ensure_image_available() {
    let mut stream = docker_client().create_image(
        Some(CreateImageOptions {
            from_image: test_image_name(),
            tag: test_image_tag(),
            ..Default::default()
        }),
        None,
        None,
    );

    while let Some(result) = tokio::time::timeout(DOCKER_API_TIMEOUT, stream.next())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "timed out pulling Ignite fixture image {}:{} after {:?}",
                test_image_name(),
                test_image_tag(),
                DOCKER_API_TIMEOUT
            )
        })
    {
        result.unwrap_or_else(|err| panic!("failed to pull Ignite fixture image: {}", err));
    }
}

fn generated_cluster_config_path(
    profile: &str,
    name_prefix: &str,
    network_name: &str,
    index: usize,
    discovery_seed_addresses: &[String],
) -> PathBuf {
    let config_root = cluster_generated_config_root(profile, name_prefix);
    fs::create_dir_all(&config_root).expect("failed to create generated cluster config dir");

    let path = config_root.join(format!("cluster-node-{index}.xml"));
    let template_path = fixture_assets_dir().join("cluster-3-node.xml");
    let mut xml = fs::read_to_string(&template_path).unwrap_or_else(|err| {
        panic!(
            "failed to read cluster fixture config {:?}: {}",
            template_path, err
        )
    });

    let discovery_node_addresses = discovery_seed_addresses
        .iter()
        .map(|addr| format!("<value>{addr}</value>"))
        .collect::<Vec<_>>()
        .join("\n                                ");
    xml = xml.replace("__DISCOVERY_NODE_ADDRESSES__", &discovery_node_addresses);

    let client_port = cluster_node_client_port(profile, index);
    let container_name = cluster_node_container_name(name_prefix, index);
    let address_resolver_property = cluster_client_address_resolver_property(
        &container_name,
        network_name,
        index,
        client_port,
    )
    .unwrap_or_default();
    xml = xml.replace("__ADDRESS_RESOLVER_PROPERTY__", &address_resolver_property);
    xml = xml.replace(
        "<property name=\"port\" value=\"10800\"/>",
        &format!("<property name=\"port\" value=\"{client_port}\"/>"),
    );

    fs::write(&path, xml).unwrap_or_else(|err| {
        panic!(
            "failed to write generated cluster config {:?}: {}",
            path, err
        )
    });
    path
}

fn cluster_discovery_seed_addresses(
    profile: &str,
    name_prefix: &str,
    network_name: &str,
    _index: usize,
) -> Vec<String> {
    let mut addresses = (0..base_cluster_node_count(profile))
        .map(|peer_index| {
            let host = cluster_node_network_ip(network_name, peer_index)
                .unwrap_or_else(|| cluster_node_container_name(name_prefix, peer_index));
            format!("{host}:47500")
        })
        .collect::<Vec<_>>();

    if addresses.is_empty() {
        addresses.push("127.0.0.1:47500".to_string());
    }

    addresses
}

fn ensure_cluster_node_runtime_config(
    profile: &str,
    name_prefix: &str,
    network_name: &str,
    index: usize,
    container_name: &str,
) {
    let discovery_seed_addresses =
        cluster_discovery_seed_addresses(profile, name_prefix, network_name, index);
    let config_path = cluster_generated_config_root(profile, name_prefix)
        .join(format!("cluster-node-{index}.xml"));
    let before = fs::read_to_string(&config_path).unwrap_or_default();
    let refreshed_path = generated_cluster_config_path(
        profile,
        name_prefix,
        network_name,
        index,
        &discovery_seed_addresses,
    );
    let after = fs::read_to_string(&refreshed_path).unwrap_or_default();

    if before != after {
        if container_is_running(container_name) {
            fixture_debug(&format!(
                "ensure_cluster_node_runtime_config: restarting {container_name} to pick up refreshed runtime config"
            ));
            restart_container(container_name);
        } else {
            fixture_debug(&format!(
                "ensure_cluster_node_runtime_config: refreshed runtime config for stopped container {container_name}"
            ));
        }
    }
}

fn cluster_client_address_resolver_property(
    container_name: &str,
    network_name: &str,
    index: usize,
    client_port: u16,
) -> Option<String> {
    let bridge_ip = container_network_ip(container_name, network_name)
        .or_else(|| cluster_node_network_ip(network_name, index))?;
    let host_addr = docker_host_addr();

    Some(format!(
        r#"<property name="addressResolver">
            <bean class="org.apache.ignite.configuration.BasicAddressResolver">
                <constructor-arg>
                    <map>
                        <entry key="{bridge_ip}:{client_port}" value="{host_addr}:{client_port}"/>
                    </map>
                </constructor-arg>
            </bean>
        </property>"#
    ))
}

fn cluster_generated_config_root(profile: &str, name_prefix: &str) -> PathBuf {
    shared_state_root()
        .join("configs")
        .join(sanitize_identifier(profile))
        .join(sanitize_identifier(name_prefix))
}

fn cluster_node_network_ip(network_name: &str, index: usize) -> Option<String> {
    let subnet = block_on_fixture(async move {
        match tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker_client().inspect_network::<String>(network_name, None),
        )
        .await
        {
            Ok(Ok(network)) => network
                .ipam
                .and_then(|ipam| ipam.config)
                .and_then(|configs| configs.into_iter().find_map(|config| config.subnet)),
            _ => None,
        }
    })?;

    cluster_static_ip_from_subnet(&subnet, index)
}

fn cluster_static_ip_from_subnet(subnet: &str, index: usize) -> Option<String> {
    let (base, _prefix) = subnet.split_once('/')?;
    let mut octets = base.parse::<Ipv4Addr>().ok()?.octets();
    let host_octet = 2u8.checked_add(index as u8)?;
    octets[3] = host_octet;
    Some(Ipv4Addr::from(octets).to_string())
}

fn acquire_file_lock(path: &Path) -> FileLockGuard {
    let started = Instant::now();

    loop {
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut lock_file) => {
                writeln!(lock_file, "{}", process::id()).expect("failed to write lock file");
                return FileLockGuard {
                    path: path.to_path_buf(),
                };
            }
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                if lock_is_stale(path) {
                    let _ = fs::remove_file(path);
                    continue;
                }

                if started.elapsed() > lock_wait_timeout() {
                    panic!("timed out waiting for Ignite test fixture lock {:?}", path);
                }

                thread::sleep(LOCK_RETRY_DELAY);
            }
            Err(err) => panic!(
                "failed to acquire Ignite test fixture lock {:?}: {}",
                path, err
            ),
        }
    }
}

fn lock_is_stale(path: &Path) -> bool {
    let Some(modified) = fs::metadata(path)
        .ok()
        .and_then(|meta| meta.modified().ok())
    else {
        return false;
    };

    SystemTime::now()
        .duration_since(modified)
        .map(|age| age > lock_stale_after())
        .unwrap_or(false)
}

fn read_shared_state(path: &Path) -> SharedContainerState {
    let Some(raw) = fs::read_to_string(path).ok() else {
        return SharedContainerState::default();
    };

    let mut lines = raw.lines();
    let ref_count = lines
        .next()
        .and_then(|line| line.trim().parse().ok())
        .unwrap_or(0);
    let mapped_port = lines.next().and_then(|line| line.trim().parse().ok());
    let owner_pids = lines
        .next()
        .map(|line| {
            line.split(',')
                .filter_map(|value| value.trim().parse::<u32>().ok())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let bootstrap_version = lines
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned);

    SharedContainerState {
        ref_count,
        mapped_port,
        owner_pids,
        bootstrap_version,
    }
}

fn write_shared_state(path: &Path, state: &SharedContainerState) {
    let owners = state
        .owner_pids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let raw = match (state.mapped_port, state.bootstrap_version.as_deref()) {
        (Some(port), Some(bootstrap_version)) => {
            format!(
                "{}\n{}\n{}\n{}\n",
                state.ref_count, port, owners, bootstrap_version
            )
        }
        (Some(port), None) => format!("{}\n{}\n{}\n\n", state.ref_count, port, owners),
        (None, Some(bootstrap_version)) => {
            format!("{}\n\n{}\n{}\n", state.ref_count, owners, bootstrap_version)
        }
        (None, None) => format!("{}\n\n{}\n\n", state.ref_count, owners),
    };
    fs::write(path, raw).expect("failed to write Ignite state file");
}

fn prune_dead_owner_pids(state: &mut SharedContainerState) {
    state.owner_pids.retain(|pid| process_is_alive(*pid));
    state.ref_count = state.owner_pids.len();
}

fn release_owner_pid(state: &mut SharedContainerState, pid: u32) -> bool {
    state.owner_pids.retain(|candidate| *candidate != pid);
    state.ref_count = state.owner_pids.len();
    state.owner_pids.is_empty()
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }

    unsafe { kill(pid as i32, 0) == 0 }
}

#[cfg(not(unix))]
fn process_is_alive(_pid: u32) -> bool {
    true
}

fn shared_state_root() -> PathBuf {
    env::temp_dir().join("ignite-rs-shared-fixtures")
}

fn shared_lock_path(root: &Path, name: &str) -> PathBuf {
    root.join(format!("{name}.lock"))
}

fn shared_state_path(root: &Path, name: &str) -> PathBuf {
    root.join(format!("{name}.state"))
}

fn test_image_name() -> String {
    env::var("IGNITE_TEST_IMAGE").unwrap_or_else(|_| DEFAULT_IGNITE_IMAGE.to_string())
}

fn test_image_tag() -> String {
    env::var("IGNITE_TEST_TAG").unwrap_or_else(|_| DEFAULT_IGNITE_TAG.to_string())
}

fn test_container_name_for(profile: &str) -> String {
    let base = env::var("IGNITE_TEST_CONTAINER_NAME")
        .unwrap_or_else(|_| format!("ignite-rs-{}", sanitize_identifier(&test_image_ref())));

    if profile == SINGLE_NODE_PROFILE {
        base
    } else if profile == SINGLE_NODE_CHURN_PROFILE {
        format!("{base}-{}-{}", sanitize_identifier(profile), process::id())
    } else {
        format!("{base}-{}", sanitize_identifier(profile))
    }
}

fn process_container_name_for(profile: &str) -> String {
    format!(
        "ignite-rs-proc-{}-{}",
        sanitize_identifier(profile),
        process::id()
    )
}

fn test_image_ref() -> String {
    format!("{}-{}", test_image_name(), test_image_tag())
}

fn managed_resource_labels(profile: &str, resource: &str) -> HashMap<String, String> {
    let mut labels = HashMap::with_capacity(3);
    labels.insert(FIXTURE_MANAGED_LABEL.to_string(), "true".to_string());
    labels.insert(FIXTURE_PROFILE_LABEL.to_string(), profile.to_string());
    labels.insert(FIXTURE_RESOURCE_LABEL.to_string(), resource.to_string());
    labels
}

fn managed_resource_labels_from_name(name: &str, resource: &str) -> HashMap<String, String> {
    let mut labels = HashMap::with_capacity(3);
    labels.insert(FIXTURE_MANAGED_LABEL.to_string(), "true".to_string());
    labels.insert(
        FIXTURE_PROFILE_LABEL.to_string(),
        profile_key_from_resource_name(name),
    );
    labels.insert(FIXTURE_RESOURCE_LABEL.to_string(), resource.to_string());
    labels
}

fn profile_key_from_resource_name(name: &str) -> String {
    for profile in [
        SINGLE_NODE_CHURN_PROFILE,
        SINGLE_NODE_AUTH_PROFILE,
        SINGLE_NODE_TLS_PROFILE,
        SINGLE_NODE_MTLS_PROFILE,
        CLUSTER_3_CHURN_PROFILE,
        CLUSTER_3_PROFILE,
        SINGLE_NODE_PROFILE,
    ] {
        if name.contains(profile) {
            return profile.to_string();
        }
    }

    "unknown".to_string()
}

fn sanitize_identifier(input: &str) -> String {
    let mut ident = String::with_capacity(input.len());

    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            ident.push(ch);
        } else {
            ident.push('-');
        }
    }

    ident
}

fn fixture_config_path(profile: &str, _index: usize) -> PathBuf {
    match profile {
        SINGLE_NODE_PROFILE | SINGLE_NODE_CHURN_PROFILE => {
            fixture_assets_dir().join("single-node.xml")
        }
        SINGLE_NODE_AUTH_PROFILE => fixture_assets_dir().join("single-node-auth.xml"),
        SINGLE_NODE_TLS_PROFILE => fixture_assets_dir().join("single-node-tls.xml"),
        SINGLE_NODE_MTLS_PROFILE => fixture_assets_dir().join("single-node-mtls.xml"),
        CLUSTER_3_PROFILE | CLUSTER_3_CHURN_PROFILE => {
            fixture_assets_dir().join("cluster-3-node.xml")
        }
        _ => fixture_assets_dir().join("single-node.xml"),
    }
}

fn shared_bootstrap_version(profile: &str) -> String {
    format!(
        "{}:{}:{}",
        profile,
        sanitize_identifier(&test_image_ref()),
        if matches!(profile, CLUSTER_3_PROFILE | CLUSTER_3_CHURN_PROFILE) {
            format!("layout-{CLUSTER_LAYOUT_VERSION}")
        } else {
            "layout-1".to_string()
        }
    )
}

fn fixture_debug(message: &str) {
    if env::var_os("IGNITE_TEST_DEBUG").is_some() {
        eprintln!("fixture debug: {message}");
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
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_START_RETRIES)
}

fn start_delay() -> Duration {
    let millis = env::var("IGNITE_TEST_START_DELAY_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_START_DELAY_MS);
    Duration::from_millis(millis)
}

fn lock_wait_timeout() -> Duration {
    env::var("IGNITE_TEST_LOCK_WAIT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_LOCK_WAIT_TIMEOUT)
}

fn lock_stale_after() -> Duration {
    env::var("IGNITE_TEST_LOCK_STALE_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_LOCK_STALE_AFTER)
}

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

fn context_registry() -> &'static Mutex<HashMap<String, Weak<IgniteContext>>> {
    CONTEXT_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_context_registry() -> std::sync::MutexGuard<'static, HashMap<String, Weak<IgniteContext>>> {
    context_registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn context_registry_key(profile: IgniteProfile, scope: FixtureScope) -> String {
    let descriptor = profile_descriptor(profile);
    let scope_key = match scope {
        FixtureScope::Process => "process",
        FixtureScope::CargoSession => "cargo-session",
    };
    format!(
        "{scope_key}:{}:{}",
        descriptor.key,
        profile_identity(profile)
    )
}

fn profile_identity(profile: IgniteProfile) -> String {
    match profile {
        IgniteProfile::DefaultSingleNode => env::var("IGNITE_ADDR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| shared_bootstrap_version(SINGLE_NODE_PROFILE)),
        IgniteProfile::SingleNodeChurn => shared_bootstrap_version(SINGLE_NODE_CHURN_PROFILE),
        IgniteProfile::ThreeNodeCluster => env::var("IGNITE_3NODE_ADDRS")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| shared_bootstrap_version(CLUSTER_3_PROFILE)),
        IgniteProfile::ThreeNodeClusterChurn => shared_bootstrap_version(CLUSTER_3_CHURN_PROFILE),
        IgniteProfile::AuthSingleNode => env::var("IGNITE_AUTH_ADDR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|addr| {
                let username = env::var("IGNITE_AUTH_USERNAME")
                    .unwrap_or_else(|_| DEFAULT_AUTH_USERNAME.to_string());
                format!("{addr}:{username}")
            })
            .unwrap_or_else(|| shared_bootstrap_version(SINGLE_NODE_AUTH_PROFILE)),
        IgniteProfile::CustomClientPort(port) => format!("127.0.0.1:{port}"),
        #[cfg(feature = "ssl")]
        IgniteProfile::TlsSingleNode => env::var("IGNITE_TLS_ADDR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|addr| {
                let server_name = env::var("IGNITE_TLS_SERVER_NAME")
                    .unwrap_or_else(|_| DEFAULT_TLS_SERVER_NAME.to_string());
                format!("{addr}:{server_name}")
            })
            .unwrap_or_else(|| shared_bootstrap_version(SINGLE_NODE_TLS_PROFILE)),
        #[cfg(feature = "ssl")]
        IgniteProfile::MtlsSingleNode => env::var("IGNITE_TLS_ADDR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|addr| {
                let server_name = env::var("IGNITE_TLS_SERVER_NAME")
                    .unwrap_or_else(|_| DEFAULT_TLS_SERVER_NAME.to_string());
                format!("{addr}:{server_name}:mtls")
            })
            .unwrap_or_else(|| shared_bootstrap_version(SINGLE_NODE_MTLS_PROFILE)),
    }
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DebugSharedState {
    pub ref_count: usize,
    pub mapped_port: Option<u16>,
    pub owner_pids: Vec<u32>,
    pub bootstrap_version: Option<String>,
}

#[cfg(test)]
impl From<SharedContainerState> for DebugSharedState {
    fn from(state: SharedContainerState) -> Self {
        Self {
            ref_count: state.ref_count,
            mapped_port: state.mapped_port,
            owner_pids: state.owner_pids,
            bootstrap_version: state.bootstrap_version,
        }
    }
}

#[cfg(test)]
impl From<DebugSharedState> for SharedContainerState {
    fn from(state: DebugSharedState) -> Self {
        Self {
            ref_count: state.ref_count,
            mapped_port: state.mapped_port,
            owner_pids: state.owner_pids,
            bootstrap_version: state.bootstrap_version,
        }
    }
}

#[cfg(test)]
pub(crate) fn debug_shared_state_roundtrip(state: DebugSharedState) -> DebugSharedState {
    let path = shared_state_root().join(format!(
        "debug-state-roundtrip-{}-{}.state",
        process::id(),
        std::thread::current().name().unwrap_or("unnamed")
    ));
    let raw_state: SharedContainerState = state.into();
    write_shared_state(&path, &raw_state);
    let roundtrip = read_shared_state(&path);
    let _ = fs::remove_file(&path);
    roundtrip.into()
}

#[cfg(test)]
pub(crate) fn debug_release_owner_pid(
    state: DebugSharedState,
    pid: u32,
) -> (DebugSharedState, bool) {
    let mut raw_state: SharedContainerState = state.into();
    let should_cleanup = release_owner_pid(&mut raw_state, pid);
    (raw_state.into(), should_cleanup)
}

#[cfg(test)]
pub(crate) fn debug_prune_dead_owner_pids(state: DebugSharedState) -> DebugSharedState {
    let mut raw_state: SharedContainerState = state.into();
    prune_dead_owner_pids(&mut raw_state);
    raw_state.into()
}

#[cfg(test)]
pub(crate) fn debug_profile_descriptor(
    profile: IgniteProfile,
) -> (&'static str, &'static str, FixtureScope) {
    let descriptor = profile_descriptor(profile);
    let kind = match descriptor.kind {
        ProfileKind::Single => "single",
        ProfileKind::Cluster => "cluster",
    };
    (descriptor.key, kind, descriptor.default_scope)
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

pub struct TestClient {
    _env: TestEnvHandle,
    inner: Client,
}

enum TestEnvHandle {
    Single(Arc<IgniteTestEnv>),
    Cluster(Arc<IgniteClusterEnv>),
}

impl Deref for TestClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

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

fn build_ignite_context(profile: IgniteProfile, scope: FixtureScope) -> IgniteContext {
    let descriptor = profile_descriptor(profile);
    let kind = match descriptor.kind {
        ProfileKind::Single => IgniteContextKind::Single(resolve_single_env(profile, scope)),
        ProfileKind::Cluster => IgniteContextKind::Cluster(resolve_cluster_env(profile, scope)),
    };

    IgniteContext {
        profile: descriptor.profile,
        scope,
        kind,
    }
}

fn resolve_single_env(profile: IgniteProfile, scope: FixtureScope) -> Arc<IgniteTestEnv> {
    match profile {
        IgniteProfile::DefaultSingleNode => {
            if let Ok(addr) = env::var("IGNITE_ADDR") {
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
            if let Ok(addr) = env::var("IGNITE_AUTH_ADDR") {
                let username = env::var("IGNITE_AUTH_USERNAME")
                    .unwrap_or_else(|_| DEFAULT_AUTH_USERNAME.to_string());
                let password = env::var("IGNITE_AUTH_PASSWORD")
                    .unwrap_or_else(|_| DEFAULT_AUTH_PASSWORD.to_string());
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
            if let Ok(addr) = env::var("IGNITE_TLS_ADDR") {
                let server_name = env::var("IGNITE_TLS_SERVER_NAME")
                    .unwrap_or_else(|_| DEFAULT_TLS_SERVER_NAME.to_string());
                let ca_pem = env::var("IGNITE_TLS_CA_PEM")
                    .expect("IGNITE_TLS_CA_PEM is required when IGNITE_TLS_ADDR is set");
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
            if let Ok(addr) = env::var("IGNITE_TLS_ADDR") {
                let server_name = env::var("IGNITE_TLS_SERVER_NAME")
                    .unwrap_or_else(|_| DEFAULT_TLS_SERVER_NAME.to_string());
                let ca_pem = env::var("IGNITE_TLS_CA_PEM")
                    .expect("IGNITE_TLS_CA_PEM is required when IGNITE_TLS_ADDR is set");
                let client_cert_pem = env::var("IGNITE_TLS_CLIENT_CERT_PEM")
                    .expect("IGNITE_TLS_CLIENT_CERT_PEM is required when IGNITE_TLS_ADDR is set");
                let client_key_pem = env::var("IGNITE_TLS_CLIENT_KEY_PEM")
                    .expect("IGNITE_TLS_CLIENT_KEY_PEM is required when IGNITE_TLS_ADDR is set");
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
    shared_env: &OnceLock<Arc<IgniteTestEnv>>,
    profile: &str,
    scope: FixtureScope,
) -> Arc<IgniteTestEnv> {
    match scope {
        FixtureScope::Process => Arc::new(IgniteTestEnv::containerized(profile, scope)),
        FixtureScope::CargoSession => shared_env
            .get_or_init(|| Arc::new(IgniteTestEnv::containerized(profile, scope)))
            .clone(),
    }
}

fn resolve_cluster_env(profile: IgniteProfile, scope: FixtureScope) -> Arc<IgniteClusterEnv> {
    if let Ok(addrs) = env::var("IGNITE_3NODE_ADDRS") {
        return Arc::new(IgniteClusterEnv::external(parse_address_list(&addrs)));
    }

    match profile {
        IgniteProfile::ThreeNodeCluster => {
            resolve_cluster_managed_env(&SHARED_CLUSTER_3_ENV, CLUSTER_3_PROFILE, scope)
        }
        IgniteProfile::ThreeNodeClusterChurn => {
            resolve_cluster_managed_env(&SHARED_CLUSTER_3_CHURN_ENV, CLUSTER_3_CHURN_PROFILE, scope)
        }
        _ => unreachable!("single-node profiles must resolve through resolve_single_env"),
    }
}

fn resolve_cluster_managed_env(
    shared_env: &OnceLock<Arc<IgniteClusterEnv>>,
    profile: &str,
    scope: FixtureScope,
) -> Arc<IgniteClusterEnv> {
    match scope {
        FixtureScope::Process => Arc::new(IgniteClusterEnv::containerized(profile, scope)),
        FixtureScope::CargoSession => shared_env
            .get_or_init(|| Arc::new(IgniteClusterEnv::containerized(profile, scope)))
            .clone(),
    }
}

pub fn ignite_test_env() -> Arc<IgniteTestEnv> {
    ignite_context(
        IgniteProfile::DefaultSingleNode,
        default_fixture_scope(IgniteProfile::DefaultSingleNode),
    )
    .single_env()
    .expect("default single-node profile resolved as cluster")
    .clone()
}

pub fn ignite_single_node_churn_env() -> Arc<IgniteTestEnv> {
    ignite_context(
        IgniteProfile::SingleNodeChurn,
        default_fixture_scope(IgniteProfile::SingleNodeChurn),
    )
    .single_env()
    .expect("single-node churn profile resolved as cluster")
    .clone()
}

pub fn ignite_auth_env() -> Arc<IgniteTestEnv> {
    ignite_context(
        IgniteProfile::AuthSingleNode,
        default_fixture_scope(IgniteProfile::AuthSingleNode),
    )
    .single_env()
    .expect("auth single-node profile resolved as cluster")
    .clone()
}

#[cfg(feature = "ssl")]
pub fn ignite_tls_env() -> Arc<IgniteTestEnv> {
    ignite_context(
        IgniteProfile::TlsSingleNode,
        default_fixture_scope(IgniteProfile::TlsSingleNode),
    )
    .single_env()
    .expect("tls single-node profile resolved as cluster")
    .clone()
}

#[cfg(feature = "ssl")]
pub fn ignite_mtls_env() -> Arc<IgniteTestEnv> {
    ignite_context(
        IgniteProfile::MtlsSingleNode,
        default_fixture_scope(IgniteProfile::MtlsSingleNode),
    )
    .single_env()
    .expect("mtls single-node profile resolved as cluster")
    .clone()
}

pub fn ignite_cluster3_env() -> Arc<IgniteClusterEnv> {
    ignite_context(
        IgniteProfile::ThreeNodeCluster,
        default_fixture_scope(IgniteProfile::ThreeNodeCluster),
    )
    .cluster_env()
    .expect("three-node profile resolved as single-node")
    .clone()
}

pub fn ignite_cluster3_churn_env() -> Arc<IgniteClusterEnv> {
    ignite_context(
        IgniteProfile::ThreeNodeClusterChurn,
        default_fixture_scope(IgniteProfile::ThreeNodeClusterChurn),
    )
    .cluster_env()
    .expect("three-node churn profile resolved as single-node")
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
        let Ok(mut client_to_server_reader) = client_stream.try_clone() else {
            return;
        };
        let Ok(mut server_to_client_writer) = server_stream.try_clone() else {
            return;
        };

        let upstream = thread::spawn(move || {
            let _ = io::copy(&mut client_to_server_reader, &mut server_to_client_writer);
            let _ = server_to_client_writer.shutdown(Shutdown::Write);
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

async fn wait_for_client_ready(conf: ClientConfig) -> IgniteResult<()> {
    let mut last_err = None;

    for _ in 0..start_retries() {
        match new_client(conf.clone()).await {
            Ok(client) => match client.get_cache_names().await {
                Ok(_) => {
                    drop(client);
                    return Ok(());
                }
                Err(err) => {
                    last_err = Some(err);
                }
            },
            Err(err) => {
                last_err = Some(err);
            }
        }

        tokio::time::sleep(start_delay()).await;
    }

    Err(last_err.expect("missing Ignite readiness failure"))
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
                        fixture_debug(&format!(
                            "wait_for_cluster_ready: request probe failed for {addr}: {err}"
                        ));
                        last_err = Some(err);
                        all_ready = false;
                        break;
                    }
                },
                Err(err) => {
                    fixture_debug(&format!(
                        "wait_for_cluster_ready: connect probe failed for {addr}: {err}"
                    ));
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
            return Err(last_err.expect("missing Ignite cluster readiness failure"));
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

pub async fn ensure_rainbow_table(client: &Client) -> IgniteResult<()> {
    let statements = rainbow_statements()?;

    for sql in statements {
        let res = match client.sql_fields::<i64>(SqlFieldsQuery::new(&sql)).await {
            Ok(cursor) => cursor.fetch_all().await,
            Err(err) => Err(err),
        };

        if let Err(err) = res {
            let msg = err.to_string();
            let acceptable = msg.contains("Table already exists") || msg.contains("Duplicate key");
            if !acceptable {
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
    .map_err(ignite_rs::error::IgniteError::from)?;

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

fn parse_address_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|addr| !addr.is_empty())
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

    Ok(ClientConfig::new(addr))
}
