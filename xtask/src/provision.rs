use anyhow::{bail, Context, Result};
use bollard::container::{
    Config as DockerContainerConfig, CreateContainerOptions, LogOutput, LogsOptions,
    NetworkingConfig, RemoveContainerOptions, StartContainerOptions, StopContainerOptions,
};
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::models::{EndpointIpamConfig, HostConfig, PortBinding};
use bollard::network::CreateNetworkOptions;
use bollard::Docker;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Constants (mirrored from fixtures.rs)
// ---------------------------------------------------------------------------

const DEFAULT_IGNITE_IMAGE: &str = "apacheignite/ignite";
const DEFAULT_IGNITE_TAG: &str = "2.17.0-arm64";
const DOCKER_API_TIMEOUT: Duration = Duration::from_secs(30);
const IMAGE_PULL_TIMEOUT: Duration = Duration::from_secs(300);
const LOG_READY_TIMEOUT: Duration = Duration::from_secs(180);
const TCP_READY_TIMEOUT: Duration = Duration::from_secs(120);

const IGNITE_PORT: u16 = 10800;
const CLUSTER_3_BASE_PORT: u16 = 12100;
const CLUSTER_3_CHURN_BASE_PORT: u16 = 12400;
const BASE_CLUSTER_NODE_COUNT: usize = 3;
const DISCOVERY_CLUSTER_NODE_COUNT: usize = 4;

const FIXTURE_MANAGED_LABEL: &str = "io.github.ignite-rs.fixture.managed";
const FIXTURE_PROFILE_LABEL: &str = "io.github.ignite-rs.fixture.profile";
const FIXTURE_RESOURCE_LABEL: &str = "io.github.ignite-rs.fixture.resource";

const CONTAINER_CONFIG_PATH: &str = "/opt/ignite/apache-ignite/config/codex-single-node.xml";
const CLUSTER_CONFIG_PATH: &str = "/opt/ignite/apache-ignite/config/codex-cluster-node.xml";
const CONTAINER_SSL_ASSETS_DIR: &str = "/opt/ignite/apache-ignite/config/codex-ssl";
const LOG_READY_MESSAGE: &str = "Topology snapshot";

// ---------------------------------------------------------------------------
// Provisioned environment
// ---------------------------------------------------------------------------

pub enum ProvisionedEnv {
    SingleNode {
        container_name: String,
        addr: String,
        profile: String,
    },
    Cluster {
        #[allow(dead_code)]
        name_prefix: String,
        network_name: String,
        node_names: Vec<String>,
        addrs: Vec<String>,
        #[allow(dead_code)]
        profile: String,
    },
    ChurnCluster {
        name_prefix: String,
        network_name: String,
        node_names: Vec<String>,
        addrs: Vec<String>,
        #[allow(dead_code)]
        profile: String,
    },
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub async fn provision_profile(
    docker: &Docker,
    profile: &str,
    workspace_root: &Path,
) -> Result<ProvisionedEnv> {
    match profile {
        "cluster-3" => provision_cluster(docker, profile, workspace_root).await,
        "cluster-3-churn" => provision_churn_cluster(docker, profile, workspace_root).await,
        _ => provision_single_node(docker, profile, workspace_root).await,
    }
}

pub async fn teardown(docker: &Docker, env: &ProvisionedEnv) -> Result<()> {
    match env {
        ProvisionedEnv::SingleNode { container_name, .. } => {
            force_remove_container(docker, container_name).await;
        }
        ProvisionedEnv::Cluster {
            node_names,
            network_name,
            ..
        }
        | ProvisionedEnv::ChurnCluster {
            node_names,
            network_name,
            ..
        } => {
            for name in node_names {
                force_remove_container(docker, name).await;
            }
            let _ =
                tokio::time::timeout(DOCKER_API_TIMEOUT, docker.remove_network(network_name)).await;
        }
    }
    Ok(())
}

pub fn provisioned_env_vars(env: &ProvisionedEnv, workspace_root: &Path) -> Vec<(String, String)> {
    match env {
        ProvisionedEnv::SingleNode { addr, profile, .. } => match profile.as_str() {
            "single-node" => vec![("IGNITE_ADDR".into(), addr.clone())],
            "single-node-auth" => vec![
                ("IGNITE_AUTH_ADDR".into(), addr.clone()),
                ("IGNITE_AUTH_USERNAME".into(), "ignite".into()),
                ("IGNITE_AUTH_PASSWORD".into(), "ignite".into()),
            ],
            "single-node-tls" => {
                let ssl_dir = ssl_fixture_assets_dir(workspace_root);
                vec![
                    ("IGNITE_TLS_ADDR".into(), addr.clone()),
                    ("IGNITE_TLS_SERVER_NAME".into(), "ignite.apache.org".into()),
                    (
                        "IGNITE_TLS_CA_PEM".into(),
                        ssl_dir.join("ca.pem").display().to_string(),
                    ),
                ]
            }
            "single-node-mtls" => {
                let ssl_dir = ssl_fixture_assets_dir(workspace_root);
                let client_pem = ssl_dir.join("client_full.pem").display().to_string();
                vec![
                    ("IGNITE_TLS_ADDR".into(), addr.clone()),
                    ("IGNITE_TLS_SERVER_NAME".into(), "ignite.apache.org".into()),
                    (
                        "IGNITE_TLS_CA_PEM".into(),
                        ssl_dir.join("ca.pem").display().to_string(),
                    ),
                    ("IGNITE_TLS_CLIENT_CERT_PEM".into(), client_pem.clone()),
                    ("IGNITE_TLS_CLIENT_KEY_PEM".into(), client_pem),
                ]
            }
            _ => vec![],
        },
        ProvisionedEnv::Cluster { addrs, .. } => {
            vec![("IGNITE_3NODE_ADDRS".into(), addrs.join(","))]
        }
        ProvisionedEnv::ChurnCluster {
            addrs, name_prefix, ..
        } => {
            // Only expose the first 3 node addresses (base nodes).
            let base_addrs: Vec<&String> = addrs.iter().take(BASE_CLUSTER_NODE_COUNT).collect();
            vec![
                (
                    "IGNITE_3NODE_CHURN_ADDRS".into(),
                    base_addrs
                        .iter()
                        .map(|a| a.as_str())
                        .collect::<Vec<_>>()
                        .join(","),
                ),
                ("IGNITE_3NODE_CHURN_PREFIX".into(), name_prefix.clone()),
            ]
        }
    }
}

// ---------------------------------------------------------------------------
// Single-node provisioning
// ---------------------------------------------------------------------------

async fn provision_single_node(
    docker: &Docker,
    profile: &str,
    workspace_root: &Path,
) -> Result<ProvisionedEnv> {
    let container_name = format!("ignite-rs-matrix-{}", sanitize_identifier(profile));
    println!("  provision: creating {container_name} (profile={profile})");

    let config_path = fixture_config_path(profile, workspace_root);
    let mut binds = vec![format!(
        "{}:{}:ro",
        config_path.display(),
        CONTAINER_CONFIG_PATH
    )];
    let mut env_vars = vec![
        format!("CONFIG_URI={CONTAINER_CONFIG_PATH}"),
        profile_jvm_opts(profile).to_string(),
    ];

    // TLS: mount SSL assets
    if profile == "single-node-tls" || profile == "single-node-mtls" {
        let ssl_dir = ssl_fixture_assets_dir(workspace_root);
        binds.push(format!(
            "{}:{}:ro",
            ssl_dir.display(),
            CONTAINER_SSL_ASSETS_DIR
        ));
    }

    // Auth: extra env vars
    if profile == "single-node-auth" {
        env_vars.push("IGNITE_ENABLE_EXPERIMENTAL_COMMAND=true".into());
        env_vars.push("OPTION_LIBS=ignite-indexing".into());
    }

    let exposed = format!("{IGNITE_PORT}/tcp");
    create_and_start_container(
        docker,
        &container_name,
        profile,
        &binds,
        &env_vars,
        Some(&exposed),
        None,
        None,
        None,
    )
    .await?;

    let port = get_mapped_port(docker, &container_name, IGNITE_PORT).await?;
    let addr = format!("{}:{}", docker_host_addr(), port);

    // Wait for log readiness (more reliable than TCP for xtask which can't do thin-client handshake).
    wait_for_container_log(
        docker,
        &container_name,
        LOG_READY_MESSAGE,
        TCP_READY_TIMEOUT,
    )
    .await?;

    // Auth profile: activate cluster via docker exec.
    if profile == "single-node-auth" {
        activate_cluster_via_exec(docker, &container_name).await?;
    }

    println!("  provision: {container_name} ready at {addr}");
    Ok(ProvisionedEnv::SingleNode {
        container_name,
        addr,
        profile: profile.to_string(),
    })
}

// ---------------------------------------------------------------------------
// Cluster provisioning
// ---------------------------------------------------------------------------

async fn provision_cluster(
    docker: &Docker,
    profile: &str,
    workspace_root: &Path,
) -> Result<ProvisionedEnv> {
    let name_prefix = format!("ignite-rs-matrix-{}", sanitize_identifier(profile));
    let network_name = format!("{name_prefix}-net");
    println!("  provision: creating cluster {name_prefix} (profile={profile})");

    // Create network.
    ensure_network(docker, &network_name, profile).await?;

    let subnet = network_subnet(docker, &network_name).await;
    let node_count = BASE_CLUSTER_NODE_COUNT;
    let mut node_names = Vec::with_capacity(node_count);
    let mut addrs = Vec::with_capacity(node_count);

    for index in 0..node_count {
        let container_name = format!("{name_prefix}-node-{index}");
        let static_ip = subnet
            .as_deref()
            .and_then(|s| cluster_static_ip_from_subnet(s, index));

        let discovery_addrs: Vec<String> = (0..node_count)
            .map(|peer| {
                let host = subnet
                    .as_deref()
                    .and_then(|s| cluster_static_ip_from_subnet(s, peer))
                    .unwrap_or_else(|| format!("{name_prefix}-node-{peer}"));
                format!("{host}:47500")
            })
            .collect();

        let client_port = CLUSTER_3_BASE_PORT + index as u16;
        let config_path = generate_cluster_config(
            profile,
            &name_prefix,
            index,
            &discovery_addrs,
            static_ip.as_deref(),
            client_port,
            workspace_root,
        )?;

        let binds = vec![format!(
            "{}:{}:ro",
            config_path.display(),
            CLUSTER_CONFIG_PATH
        )];
        let env_vars = vec![
            format!("CONFIG_URI={CLUSTER_CONFIG_PATH}"),
            profile_jvm_opts(profile).to_string(),
        ];
        let port_spec = format!("{client_port}/tcp");

        let port_bindings = HashMap::from([(
            port_spec.clone(),
            Some(vec![PortBinding {
                host_ip: Some("127.0.0.1".to_string()),
                host_port: Some(client_port.to_string()),
            }]),
        )]);

        create_and_start_container(
            docker,
            &container_name,
            profile,
            &binds,
            &env_vars,
            Some(&port_spec),
            Some(&network_name),
            static_ip.as_deref(),
            Some(port_bindings),
        )
        .await?;

        wait_for_container_log(
            docker,
            &container_name,
            LOG_READY_MESSAGE,
            LOG_READY_TIMEOUT,
        )
        .await?;

        addrs.push(format!("{}:{}", docker_host_addr(), client_port));
        node_names.push(container_name);
    }

    println!(
        "  provision: cluster {name_prefix} ready at {}",
        addrs.join(", ")
    );
    Ok(ProvisionedEnv::Cluster {
        name_prefix,
        network_name,
        node_names,
        addrs,
        profile: profile.to_string(),
    })
}

// ---------------------------------------------------------------------------
// Churn-cluster provisioning (4 nodes, node 3 stopped after startup)
// ---------------------------------------------------------------------------

async fn provision_churn_cluster(
    docker: &Docker,
    profile: &str,
    workspace_root: &Path,
) -> Result<ProvisionedEnv> {
    let name_prefix = format!("ignite-rs-matrix-{}", sanitize_identifier(profile));
    let network_name = format!("{name_prefix}-net");
    println!("  provision: creating churn cluster {name_prefix} (profile={profile})");

    ensure_network(docker, &network_name, profile).await?;

    let subnet = network_subnet(docker, &network_name).await;
    let node_count = DISCOVERY_CLUSTER_NODE_COUNT;
    let mut node_names = Vec::with_capacity(node_count);
    let mut addrs = Vec::with_capacity(node_count);

    for index in 0..node_count {
        let container_name = format!("{name_prefix}-node-{index}");
        let static_ip = subnet
            .as_deref()
            .and_then(|s| cluster_static_ip_from_subnet(s, index));

        let discovery_addrs: Vec<String> = (0..BASE_CLUSTER_NODE_COUNT)
            .map(|peer| {
                let host = subnet
                    .as_deref()
                    .and_then(|s| cluster_static_ip_from_subnet(s, peer))
                    .unwrap_or_else(|| format!("{name_prefix}-node-{peer}"));
                format!("{host}:47500")
            })
            .collect();

        let client_port = CLUSTER_3_CHURN_BASE_PORT + index as u16;
        let config_path = generate_cluster_config(
            profile,
            &name_prefix,
            index,
            &discovery_addrs,
            static_ip.as_deref(),
            client_port,
            workspace_root,
        )?;

        let binds = vec![format!(
            "{}:{}:ro",
            config_path.display(),
            CLUSTER_CONFIG_PATH
        )];
        let env_vars = vec![
            format!("CONFIG_URI={CLUSTER_CONFIG_PATH}"),
            profile_jvm_opts(profile).to_string(),
        ];
        let port_spec = format!("{client_port}/tcp");

        let port_bindings = HashMap::from([(
            port_spec.clone(),
            Some(vec![PortBinding {
                host_ip: Some("127.0.0.1".to_string()),
                host_port: Some(client_port.to_string()),
            }]),
        )]);

        create_and_start_container(
            docker,
            &container_name,
            profile,
            &binds,
            &env_vars,
            Some(&port_spec),
            Some(&network_name),
            static_ip.as_deref(),
            Some(port_bindings),
        )
        .await?;

        wait_for_container_log(
            docker,
            &container_name,
            LOG_READY_MESSAGE,
            LOG_READY_TIMEOUT,
        )
        .await?;

        addrs.push(format!("{}:{}", docker_host_addr(), client_port));
        node_names.push(container_name);
    }

    // Stop the extra node (node 3) — only base nodes should be running initially.
    for index in BASE_CLUSTER_NODE_COUNT..node_count {
        let name = &node_names[index];
        println!("  provision: stopping extra node {name}");
        let _ = tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker.stop_container(name, Some(StopContainerOptions { t: 1 })),
        )
        .await;
    }

    println!(
        "  provision: churn cluster {name_prefix} ready at {}",
        addrs
            .iter()
            .take(BASE_CLUSTER_NODE_COUNT)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok(ProvisionedEnv::ChurnCluster {
        name_prefix,
        network_name,
        node_names,
        addrs,
        profile: profile.to_string(),
    })
}

// ---------------------------------------------------------------------------
// Docker helpers
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn create_and_start_container(
    docker: &Docker,
    name: &str,
    profile: &str,
    binds: &[String],
    env_vars: &[String],
    exposed_port: Option<&str>,
    network_name: Option<&str>,
    static_ip: Option<&str>,
    explicit_port_bindings: Option<HashMap<String, Option<Vec<PortBinding>>>>,
) -> Result<()> {
    let image = format!("{}:{}", test_image_name(), test_image_tag());

    let exposed_ports = exposed_port.map(|p| HashMap::from([(p.to_string(), HashMap::new())]));

    let port_bindings = explicit_port_bindings;
    let publish_all = port_bindings.is_none() && exposed_port.is_some();

    let networking_config = network_name.map(|net| {
        let endpoint = bollard::models::EndpointSettings {
            ipam_config: static_ip.map(|ip| EndpointIpamConfig {
                ipv4_address: Some(ip.to_string()),
                ..Default::default()
            }),
            aliases: Some(vec![name.to_string()]),
            ..Default::default()
        };
        NetworkingConfig {
            endpoints_config: HashMap::from([(net.to_string(), endpoint)]),
        }
    });

    let config = DockerContainerConfig {
        image: Some(image.clone()),
        env: Some(env_vars.to_vec()),
        labels: Some(managed_resource_labels(profile)),
        host_config: Some(HostConfig {
            binds: Some(binds.to_vec()),
            network_mode: network_name.map(String::from),
            port_bindings,
            publish_all_ports: Some(publish_all),
            ..Default::default()
        }),
        networking_config,
        exposed_ports,
        ..Default::default()
    };

    // Remove any existing container with same name.
    force_remove_container(docker, name).await;

    // Create (pull image on first failure).
    match tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker.create_container(Some(CreateContainerOptions { name }), config.clone()),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(err)) => {
            let msg = err.to_string();
            if msg.contains("No such image") || msg.contains("not found") {
                ensure_image_available(docker).await?;
                tokio::time::timeout(
                    DOCKER_API_TIMEOUT,
                    docker.create_container(Some(CreateContainerOptions { name }), config),
                )
                .await
                .context("timed out creating container after image pull")?
                .context("failed to create container after image pull")?;
            } else {
                bail!("failed to create container {name}: {err}");
            }
        }
        Err(_) => bail!("timed out creating container {name}"),
    }

    // Start.
    tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker.start_container::<String>(name, None::<StartContainerOptions<String>>),
    )
    .await
    .with_context(|| format!("timed out starting container {name}"))?
    .or_else(|err| {
        if err.to_string().contains("is already started") {
            Ok(())
        } else {
            Err(err)
        }
    })
    .with_context(|| format!("failed to start container {name}"))?;

    Ok(())
}

async fn force_remove_container(docker: &Docker, name: &str) {
    let _ = tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker.remove_container(
            name,
            Some(RemoveContainerOptions {
                force: true,
                v: true,
                ..Default::default()
            }),
        ),
    )
    .await;
}

async fn ensure_image_available(docker: &Docker) -> Result<()> {
    let image = test_image_name();
    let tag = test_image_tag();
    println!("  provision: pulling image {image}:{tag}");

    let mut stream = docker.create_image(
        Some(bollard::image::CreateImageOptions {
            from_image: image.clone(),
            tag: tag.clone(),
            ..Default::default()
        }),
        None,
        None,
    );

    let deadline = tokio::time::Instant::now() + IMAGE_PULL_TIMEOUT;
    loop {
        let remaining = deadline - tokio::time::Instant::now();
        if remaining.is_zero() {
            bail!("timed out pulling image {image}:{tag}");
        }
        match tokio::time::timeout(remaining.min(DOCKER_API_TIMEOUT), stream.next()).await {
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(err))) => bail!("failed to pull image {image}:{tag}: {err}"),
            Ok(None) => break,
            Err(_) => bail!("timed out pulling image {image}:{tag} (no progress)"),
        }
    }
    Ok(())
}

async fn get_mapped_port(docker: &Docker, name: &str, container_port: u16) -> Result<u16> {
    let port_spec = format!("{container_port}/tcp");
    let inspect = tokio::time::timeout(DOCKER_API_TIMEOUT, docker.inspect_container(name, None))
        .await
        .with_context(|| format!("timed out inspecting container {name}"))?
        .with_context(|| format!("failed to inspect container {name}"))?;

    inspect
        .network_settings
        .and_then(|ns| ns.ports)
        .and_then(|ports| ports.get(&port_spec).cloned())
        .and_then(|bindings| bindings?.into_iter().next())
        .and_then(|b| b.host_port?.parse().ok())
        .with_context(|| format!("no mapped port {port_spec} for {name}"))
}

async fn wait_for_container_log(
    docker: &Docker,
    name: &str,
    message: &str,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    let poll_interval = Duration::from_secs(2);

    loop {
        let mut logs = String::new();
        let mut stream = docker.logs::<String>(
            name,
            Some(LogsOptions {
                follow: false,
                stdout: true,
                stderr: true,
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
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("timed out waiting for '{message}' in {name} logs after {timeout:?}");
        }
        tokio::time::sleep(poll_interval).await;
    }
}

async fn ensure_network(docker: &Docker, name: &str, profile: &str) -> Result<()> {
    let labels = HashMap::from([
        (FIXTURE_MANAGED_LABEL.to_string(), "true".to_string()),
        (FIXTURE_PROFILE_LABEL.to_string(), profile.to_string()),
        (FIXTURE_RESOURCE_LABEL.to_string(), "network".to_string()),
    ]);

    match tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker.create_network(CreateNetworkOptions {
            name: name.to_string(),
            check_duplicate: true,
            labels,
            ..Default::default()
        }),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(err)) => {
            if err.to_string().contains("already exists") {
                Ok(())
            } else {
                bail!("failed to create network {name}: {err}")
            }
        }
        Err(_) => bail!("timed out creating network {name}"),
    }
}

async fn network_subnet(docker: &Docker, name: &str) -> Option<String> {
    let inspect = tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker.inspect_network::<String>(name, None),
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

async fn activate_cluster_via_exec(docker: &Docker, container_name: &str) -> Result<()> {
    println!("  provision: activating cluster on {container_name}");

    // Retry activation — the cluster may still be initializing.
    let mut last_err = None;
    for attempt in 0..40 {
        let exec = docker
            .create_exec(
                container_name,
                CreateExecOptions {
                    cmd: Some(vec![
                        "/opt/ignite/apache-ignite/bin/control.sh",
                        "--set-state",
                        "ACTIVE",
                        "--user",
                        "ignite",
                        "--password",
                        "ignite",
                        "--yes",
                    ]),
                    attach_stdout: Some(true),
                    attach_stderr: Some(true),
                    ..Default::default()
                },
            )
            .await;

        let exec_id = match exec {
            Ok(resp) => resp.id,
            Err(err) => {
                last_err = Some(format!("create exec failed: {err}"));
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };

        match docker.start_exec(&exec_id, None).await {
            Ok(StartExecResults::Attached { mut output, .. }) => {
                let mut stdout = String::new();
                while let Some(Ok(msg)) = output.next().await {
                    match msg {
                        LogOutput::StdOut { message } | LogOutput::StdErr { message } => {
                            stdout.push_str(&String::from_utf8_lossy(&message));
                        }
                        _ => {}
                    }
                }
                // Check exec exit code.
                if let Ok(info) = docker.inspect_exec(&exec_id).await {
                    if info.exit_code == Some(0) {
                        return Ok(());
                    }
                    last_err = Some(format!(
                        "control.sh exited with code {:?}: {stdout}",
                        info.exit_code
                    ));
                } else {
                    last_err = Some(format!("control.sh output: {stdout}"));
                }
            }
            Ok(StartExecResults::Detached) => {
                return Ok(());
            }
            Err(err) => {
                last_err = Some(format!("start exec failed: {err}"));
            }
        }

        if attempt < 39 {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    bail!(
        "failed to activate cluster on {container_name}: {}",
        last_err.unwrap_or_else(|| "unknown error".into())
    )
}

// ---------------------------------------------------------------------------
// Path / config helpers
// ---------------------------------------------------------------------------

fn fixture_config_path(profile: &str, workspace_root: &Path) -> PathBuf {
    let dir = workspace_root.join("ignite-rs/tests/common/fixtures/ignite");
    match profile {
        "single-node-auth" => dir.join("single-node-auth.xml"),
        "single-node-tls" | "single-node-mtls" => dir.join("single-node-tls.xml"),
        _ => dir.join("single-node.xml"),
    }
}

fn ssl_fixture_assets_dir(workspace_root: &Path) -> PathBuf {
    workspace_root
        .join("..")
        .join("ignite")
        .join("modules")
        .join("platforms")
        .join("cpp")
        .join("odbc-test")
        .join("config")
        .join("ssl")
}

fn generate_cluster_config(
    profile: &str,
    name_prefix: &str,
    index: usize,
    discovery_addrs: &[String],
    static_ip: Option<&str>,
    client_port: u16,
    workspace_root: &Path,
) -> Result<PathBuf> {
    let config_root = workspace_root
        .join("target")
        .join("ignite-rs-fixtures")
        .join("configs")
        .join(sanitize_identifier(profile))
        .join(sanitize_identifier(name_prefix));
    fs::create_dir_all(&config_root).context("failed to create cluster config dir")?;

    let path = config_root.join(format!("cluster-node-{index}.xml"));
    let template_path =
        workspace_root.join("ignite-rs/tests/common/fixtures/ignite/cluster-3-node.xml");
    let mut xml = fs::read_to_string(&template_path).with_context(|| {
        format!(
            "failed to read cluster config template: {}",
            template_path.display()
        )
    })?;

    let discovery_xml = discovery_addrs
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

    fs::write(&path, xml).context("failed to write cluster config")?;
    Ok(path)
}

fn profile_jvm_opts(profile: &str) -> &'static str {
    // Shared containers serve all suites in a bucket sequentially, accumulating
    // caches and state. They need more heap than per-test containers.
    match profile {
        "cluster-3" | "cluster-3-churn" => "JVM_OPTS=-Xms512m -Xmx512m -DIGNITE_QUIET=false",
        _ => "JVM_OPTS=-Xms1024m -Xmx1024m -DIGNITE_QUIET=false",
    }
}

fn managed_resource_labels(profile: &str) -> HashMap<String, String> {
    HashMap::from([
        (FIXTURE_MANAGED_LABEL.to_string(), "true".to_string()),
        (FIXTURE_PROFILE_LABEL.to_string(), profile.to_string()),
        (FIXTURE_RESOURCE_LABEL.to_string(), "container".to_string()),
    ])
}

fn test_image_name() -> String {
    env::var("IGNITE_TEST_IMAGE").unwrap_or_else(|_| DEFAULT_IGNITE_IMAGE.to_string())
}

fn test_image_tag() -> String {
    env::var("IGNITE_TEST_TAG").unwrap_or_else(|_| DEFAULT_IGNITE_TAG.to_string())
}

fn docker_host_addr() -> String {
    env::var("TESTCONTAINERS_HOST_OVERRIDE")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "127.0.0.1".to_string())
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
