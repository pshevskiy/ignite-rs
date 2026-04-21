mod provision;

use anyhow::{bail, Context, Result};
use bollard::container::{ListContainersOptions, RemoveContainerOptions};
use bollard::network::ListNetworksOptions;
use bollard::Docker;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};

const FIXTURE_MANAGED_LABEL: &str = "io.github.ignite-rs.fixture.managed";
const FIXTURE_PROFILE_LABEL: &str = "io.github.ignite-rs.fixture.profile";
const WORKSPACE_MANIFEST: &str = "Cargo.toml";
const CLIENT_PACKAGE: &str = "ignite-rs";

fn main() -> Result<()> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("test-matrix") => {
            let bucket = parse_bucket_arg(args.collect::<Vec<_>>().as_slice())?;
            run_test_matrix(bucket)
        }
        Some(other) => bail!("unknown xtask command: {other}"),
        None => bail!("usage: cargo run --manifest-path ignite-rs/Cargo.toml -p xtask -- test-matrix [--bucket <name>]"),
    }
}

fn parse_bucket_arg(args: &[String]) -> Result<Option<Bucket>> {
    match args {
        [] => Ok(None),
        [flag, value] if flag == "--bucket" => Ok(Some(Bucket::parse(value)?)),
        _ => bail!(
            "usage: test-matrix [--bucket <pure|single_node|cluster3|cluster3_churn|auth|ssl|parity>]"
        ),
    }
}

fn run_test_matrix(bucket_filter: Option<Bucket>) -> Result<()> {
    let workspace_root = workspace_root();
    let matrix = load_matrix(&workspace_root)?;

    let mut stages = Vec::new();
    if let Some(bucket) = bucket_filter {
        stages.push(bucket);
    } else {
        stages.extend(Bucket::ordered());
    }

    // Global cleanup: remove ALL managed fixture containers from previous runs
    // to prevent stale containers from blocking provisioning.
    let all_profiles: BTreeSet<String> = KNOWN_PROFILES
        .iter()
        .filter(|p| **p != "none")
        .map(|p| p.to_string())
        .collect();
    cleanup_profiles(&workspace_root, &all_profiles).context("failed initial global cleanup")?;

    run_cargo_check(&workspace_root)?;

    let needs_non_ssl = stages.iter().any(|bucket| !bucket.requires_ssl_feature());
    let needs_ssl = stages.iter().any(|bucket| bucket.requires_ssl_feature());

    if needs_non_ssl {
        run_compile_stage(&workspace_root, false)?;
        run_unit_stage(&workspace_root, false)?;
    }

    if !stages.is_empty() {
        for bucket in stages
            .iter()
            .copied()
            .filter(|bucket| !bucket.requires_ssl_feature())
        {
            run_bucket(&workspace_root, &matrix, bucket)?;
        }
    }

    if needs_ssl {
        run_compile_stage(&workspace_root, true)?;
        run_unit_stage(&workspace_root, true)?;
        for bucket in stages
            .iter()
            .copied()
            .filter(|bucket| bucket.requires_ssl_feature())
        {
            run_bucket(&workspace_root, &matrix, bucket)?;
        }
    }

    Ok(())
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask must live directly under the workspace root")
        .to_path_buf()
}

fn load_matrix(workspace_root: &Path) -> Result<TestMatrix> {
    let path = workspace_root.join("ignite-rs/tests/test_matrix.toml");
    let raw = fs::read_to_string(&path)
        .with_context(|| format!("failed to read test matrix {}", path.display()))?;
    let matrix: TestMatrix = toml::from_str(&raw)
        .with_context(|| format!("failed to parse test matrix {}", path.display()))?;
    matrix.validate()?;
    Ok(matrix)
}

fn build_parity_driver(workspace_root: &Path) -> Result<()> {
    let driver_dir = workspace_root.join("tests/java-parity-driver");
    if !driver_dir.exists() {
        eprintln!(
            "warning: parity driver dir {} missing — skipping JAR build",
            driver_dir.display()
        );
        return Ok(());
    }
    // Is `mvn` available?
    let mvn_probe = Command::new("mvn").arg("--version").output();
    if mvn_probe.is_err() {
        eprintln!(
            "warning: `mvn` not found on PATH — skipping parity driver JAR build. \
             Install Maven to exercise Tier-2 parity; tests detect missing JAR and skip."
        );
        return Ok(());
    }
    println!("==> pre-flight: building Java parity driver JAR");
    let status = Command::new("mvn")
        .arg("-q")
        .arg("-e")
        .arg("package")
        .current_dir(&driver_dir)
        .status();
    match status {
        Ok(s) if s.success() => {
            println!("==> parity driver JAR built");
            Ok(())
        }
        Ok(s) => {
            eprintln!(
                "warning: `mvn package` exited with {} — parity tests will skip. \
                 Fix the JAR build to enable Tier-2.",
                s
            );
            Ok(())
        }
        Err(e) => {
            eprintln!(
                "warning: failed to start `mvn`: {} — parity tests will skip",
                e
            );
            Ok(())
        }
    }
}

fn run_cargo_check(workspace_root: &Path) -> Result<()> {
    run_command(
        workspace_root,
        "cargo check",
        &["cargo", "check", "--manifest-path", WORKSPACE_MANIFEST],
        &[],
    )
}

fn run_compile_stage(workspace_root: &Path, ssl: bool) -> Result<()> {
    let mut args = vec![
        "cargo",
        "test",
        "--manifest-path",
        WORKSPACE_MANIFEST,
        "--tests",
        "--no-run",
    ];
    if ssl {
        args.push("--features");
        args.push("ssl");
    }
    run_command(
        workspace_root,
        if ssl {
            "cargo test --features ssl --tests --no-run"
        } else {
            "cargo test --tests --no-run"
        },
        &args,
        &[],
    )
}

fn run_unit_stage(workspace_root: &Path, ssl: bool) -> Result<()> {
    let mut args = vec![
        "cargo",
        "test",
        "--manifest-path",
        WORKSPACE_MANIFEST,
        "--package",
        CLIENT_PACKAGE,
        "--lib",
        "--bins",
        "--examples",
    ];
    if ssl {
        args.push("--features");
        args.push("ssl");
    }
    run_command(
        workspace_root,
        if ssl {
            "cargo test --package ignite-rs --lib --bins --examples --features ssl"
        } else {
            "cargo test --package ignite-rs --lib --bins --examples"
        },
        &args,
        &[],
    )
}

fn run_bucket(workspace_root: &Path, matrix: &TestMatrix, bucket: Bucket) -> Result<()> {
    let suites = matrix.bucket(bucket);
    if suites.is_empty() {
        bail!(
            "bucket {} has no suites in the test matrix",
            bucket.as_str()
        );
    }

    // Pre-flight: the parity bucket requires the Java driver JAR. Build it
    // via `mvn` before any test runs. If mvn is missing, print a skip note
    // and continue — parity tests detect a missing JAR at runtime and skip.
    if bucket == Bucket::Parity {
        build_parity_driver(workspace_root)?;
    }

    let live_profiles = suites
        .iter()
        .filter_map(|suite| suite.live_profile())
        .collect::<BTreeSet<_>>();
    let base_envs = bucket_env(bucket);

    if !live_profiles.is_empty() {
        cleanup_profiles(workspace_root, &live_profiles)
            .with_context(|| format!("failed to clean fixtures before {}", bucket.as_str()))?;
    }

    // Determine which profiles can be shared: a profile is eligible only if
    // ALL suites using it in this bucket have scope = "cargo_session".
    let shared_profiles = {
        let mut candidates: BTreeSet<String> = BTreeSet::new();
        let mut excluded: BTreeSet<String> = BTreeSet::new();
        for suite in &suites {
            if let Some(profile) = suite.live_profile() {
                if suite.is_shared_scope() {
                    candidates.insert(profile);
                } else {
                    excluded.insert(profile);
                }
            }
        }
        candidates.retain(|p| !excluded.contains(p));
        candidates
    };

    // Provision shared containers.
    let runtime = tokio::runtime::Runtime::new().context("failed to start provisioning runtime")?;
    let docker = connect_docker().context("failed to connect to Docker for provisioning")?;
    let mut provisioned: std::collections::HashMap<String, provision::ProvisionedEnv> =
        std::collections::HashMap::new();

    if !shared_profiles.is_empty() {
        println!(
            "==> provisioning shared containers for bucket {} (profiles: {})",
            bucket.as_str(),
            shared_profiles
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        );
        for profile in &shared_profiles {
            let env = runtime
                .block_on(provision::provision_profile(
                    &docker,
                    profile,
                    workspace_root,
                ))
                .with_context(|| {
                    format!("failed to provision shared container for profile {profile}")
                })?;
            provisioned.insert(profile.clone(), env);
        }
    }

    // Build per-profile env var overrides.
    let profile_envs: std::collections::HashMap<String, Vec<(String, String)>> = provisioned
        .iter()
        .map(|(profile, env)| {
            (
                profile.clone(),
                provision::provisioned_env_vars(env, workspace_root),
            )
        })
        .collect();

    // Group suites by key to batch exact-match invocations.
    let suite_result: Result<()> = (|| {
        // Partition suites into groups that can be batched (same test binary,
        // profile, scope, features, serial) and standalone entries.
        let mut groups: BTreeMap<SuiteGroupKey, Vec<&SuiteEntry>> = BTreeMap::new();
        let mut standalone: Vec<&SuiteEntry> = Vec::new();

        for suite in &suites {
            if suite.exact.is_some() {
                let key = SuiteGroupKey::from(suite);
                groups.entry(key).or_default().push(suite);
            } else {
                standalone.push(suite);
            }
        }

        // Run batched exact-match groups.
        for (_, group) in &groups {
            let mut envs = base_envs.clone();
            let representative = group[0];
            if let Some(extra) = profile_envs.get(&representative.profile) {
                envs.extend(extra.iter().cloned());
            }

            if group.len() >= 2 {
                let exact_names: Vec<&str> =
                    group.iter().filter_map(|s| s.exact.as_deref()).collect();
                run_suite_group(workspace_root, bucket, representative, &exact_names, &envs)
                    .with_context(|| {
                        format!(
                            "bucket={} test={} profile={} features={:?} (batched {} tests)",
                            bucket.as_str(),
                            representative.test,
                            representative.profile,
                            representative.features,
                            exact_names.len(),
                        )
                    })?;
            } else {
                run_suite(workspace_root, bucket, representative, &envs).with_context(|| {
                    format!(
                        "bucket={} test={} profile={} features={:?} exact={}",
                        bucket.as_str(),
                        representative.test,
                        representative.profile,
                        representative.features,
                        representative.exact.as_deref().unwrap_or("<none>")
                    )
                })?;
            }
        }

        // Run standalone (non-exact) suites.
        for suite in &standalone {
            let mut envs = base_envs.clone();
            if let Some(extra) = profile_envs.get(&suite.profile) {
                envs.extend(extra.iter().cloned());
            }
            run_suite(workspace_root, bucket, suite, &envs).with_context(|| {
                format!(
                    "bucket={} test={} profile={} features={:?} exact={}",
                    bucket.as_str(),
                    suite.test,
                    suite.profile,
                    suite.features,
                    suite.exact.as_deref().unwrap_or("<none>")
                )
            })?;
        }
        Ok(())
    })();

    // Always teardown provisioned containers (even on suite failure).
    if !provisioned.is_empty() {
        println!(
            "==> tearing down shared containers for bucket {}",
            bucket.as_str()
        );
        for (profile, env) in &provisioned {
            if let Err(err) = runtime.block_on(provision::teardown(&docker, env)) {
                eprintln!("warning: failed to teardown {profile}: {err}");
            }
        }
    }

    // Clean up both profile-specific and any stale fixture containers left
    // by in-process test fixtures (e.g., single-node containers created by
    // connect() inside cluster3_churn tests).
    let all_profiles: BTreeSet<String> = KNOWN_PROFILES
        .iter()
        .filter(|p| **p != "none")
        .map(|p| p.to_string())
        .collect();
    cleanup_profiles(workspace_root, &all_profiles)
        .with_context(|| format!("failed to clean fixtures after {}", bucket.as_str()))?;

    suite_result
}

fn run_suite(
    workspace_root: &Path,
    bucket: Bucket,
    suite: &SuiteEntry,
    envs: &[(String, String)],
) -> Result<()> {
    let mut args = vec![
        "cargo".to_string(),
        "test".to_string(),
        "--manifest-path".to_string(),
        WORKSPACE_MANIFEST.to_string(),
        "--package".to_string(),
        CLIENT_PACKAGE.to_string(),
    ];
    if !suite.features.is_empty() {
        args.push("--features".to_string());
        args.push(suite.features.join(" "));
    }
    args.push("--test".to_string());
    args.push(suite.test.clone());
    if let Some(exact) = &suite.exact {
        args.push(exact.clone());
    }
    args.push("--".to_string());
    if suite.exact.is_some() {
        args.push("--exact".to_string());
    }
    if bucket.is_live() || suite.serial {
        args.push("--test-threads=1".to_string());
    }
    if bucket.is_live() {
        args.push("--nocapture".to_string());
    }

    let label = format!(
        "bucket={} test={} profile={} exact={}",
        bucket.as_str(),
        suite.test,
        suite.profile,
        suite.exact.as_deref().unwrap_or("<none>")
    );

    let arg_refs = args.iter().map(String::as_str).collect::<Vec<_>>();
    run_command(workspace_root, &label, &arg_refs, envs)
}

fn run_suite_group(
    workspace_root: &Path,
    bucket: Bucket,
    representative: &SuiteEntry,
    exact_names: &[&str],
    envs: &[(String, String)],
) -> Result<()> {
    let mut args = vec![
        "cargo".to_string(),
        "test".to_string(),
        "--manifest-path".to_string(),
        WORKSPACE_MANIFEST.to_string(),
        "--package".to_string(),
        CLIENT_PACKAGE.to_string(),
    ];
    if !representative.features.is_empty() {
        args.push("--features".to_string());
        args.push(representative.features.join(" "));
    }
    args.push("--test".to_string());
    args.push(representative.test.clone());

    // Build a regex filter: ^name1$|^name2$|...
    let filter = exact_names
        .iter()
        .map(|name| format!("^{name}$"))
        .collect::<Vec<_>>()
        .join("|");
    args.push(filter);

    args.push("--".to_string());
    if bucket.is_live() || representative.serial {
        args.push("--test-threads=1".to_string());
    }
    if bucket.is_live() {
        args.push("--nocapture".to_string());
    }

    let label = format!(
        "bucket={} test={} profile={} (batched {} exact tests)",
        bucket.as_str(),
        representative.test,
        representative.profile,
        exact_names.len(),
    );

    let arg_refs = args.iter().map(String::as_str).collect::<Vec<_>>();
    run_command(workspace_root, &label, &arg_refs, envs)
}

fn bucket_env(bucket: Bucket) -> Vec<(String, String)> {
    let mut envs = vec![("CARGO_TERM_COLOR".to_string(), "always".to_string())];
    if bucket.is_live() {
        envs.push((
            "IGNITE_TEST_CONTAINER_NAME".to_string(),
            format!("ignite-rs-matrix-{}", bucket.as_str().replace('_', "-")),
        ));
        envs.push((
            "IGNITE_TEST_LOCK_STALE_MS".to_string(),
            env::var("IGNITE_TEST_LOCK_STALE_MS").unwrap_or_else(|_| "1000".to_string()),
        ));
    }
    envs
}

fn run_command(
    workspace_root: &Path,
    label: &str,
    args: &[&str],
    envs: &[(String, String)],
) -> Result<()> {
    println!("==> {label}");
    println!("$ {}", args.join(" "));

    let mut command = Command::new(args[0]);
    command.args(&args[1..]).current_dir(workspace_root);
    for (key, value) in envs {
        command.env(key, value);
    }

    let status = command
        .status()
        .with_context(|| format!("failed to start command for {label}"))?;
    ensure_success(status, args, label)
}

fn ensure_success(status: ExitStatus, args: &[&str], label: &str) -> Result<()> {
    if status.success() {
        return Ok(());
    }

    bail!(
        "command failed for {label}: {} (exit status: {status})",
        args.join(" ")
    )
}

fn cleanup_profiles(workspace_root: &Path, profiles: &BTreeSet<String>) -> Result<()> {
    let runtime = tokio::runtime::Runtime::new().context("failed to start cleanup runtime")?;
    runtime.block_on(async move {
        let docker = connect_docker().context("failed to connect to Docker-compatible API")?;
        for profile in profiles {
            cleanup_profile_resources(&docker, profile).await?;
        }
        Ok::<(), anyhow::Error>(())
    })?;

    let state_root = workspace_root
        .join("..")
        .canonicalize()
        .unwrap_or_else(|_| workspace_root.to_path_buf());
    cleanup_state_files(&state_root, profiles)?;
    Ok(())
}

fn cleanup_state_files(_workspace_root: &Path, profiles: &BTreeSet<String>) -> Result<()> {
    let shared_root = env::temp_dir().join("ignite-rs-shared-fixtures");
    for profile in profiles {
        let _ = fs::remove_file(shared_root.join(format!("{profile}.lock")));
        let _ = fs::remove_file(shared_root.join(format!("{profile}.state")));
        let _ = fs::remove_dir_all(
            shared_root
                .join("configs")
                .join(sanitize_identifier(profile)),
        );
    }
    Ok(())
}

async fn cleanup_profile_resources(docker: &Docker, profile: &str) -> Result<()> {
    let containers = docker
        .list_containers(Some(ListContainersOptions::<String> {
            all: true,
            ..Default::default()
        }))
        .await
        .with_context(|| format!("failed to list containers for profile {profile}"))?;

    for container in containers {
        let labels = container.labels.unwrap_or_default();
        let names = container.names.unwrap_or_default();
        let is_managed = labels
            .get(FIXTURE_MANAGED_LABEL)
            .map(|value| value == "true")
            .unwrap_or(false);
        let matches_label = labels
            .get(FIXTURE_PROFILE_LABEL)
            .map(|value| value == profile)
            .unwrap_or(false);

        // Only clean up containers that are both managed AND belong to this profile.
        if !is_managed || !matches_label {
            continue;
        }

        let id = container
            .id
            .clone()
            .or_else(|| names.first().cloned())
            .unwrap_or_else(|| profile.to_string());

        let _ = docker
            .remove_container(
                &id,
                Some(RemoveContainerOptions {
                    force: true,
                    v: true,
                    ..Default::default()
                }),
            )
            .await;
    }

    let networks = docker
        .list_networks::<String>(Some(ListNetworksOptions {
            ..Default::default()
        }))
        .await
        .with_context(|| format!("failed to list networks for profile {profile}"))?;

    for network in networks {
        let labels = network.labels.unwrap_or_default();
        let name = network.name.unwrap_or_default();
        let matches_profile = labels
            .get(FIXTURE_PROFILE_LABEL)
            .map(|value| value == profile)
            .unwrap_or(false);

        if !matches_profile {
            continue;
        }

        let network_id = network.id.unwrap_or(name);
        let _ = docker.remove_network(&network_id).await;
    }

    Ok(())
}

fn connect_docker() -> Result<Docker> {
    if let Ok(host) = env::var("DOCKER_HOST") {
        if let Some(path) = host.strip_prefix("unix://") {
            Ok(Docker::connect_with_unix(
                path,
                120,
                bollard::API_DEFAULT_VERSION,
            )?)
        } else {
            Ok(Docker::connect_with_http_defaults()?)
        }
    } else {
        // Try Colima socket first (macOS), then local/http defaults.
        if let Some(home) = env::var_os("HOME") {
            let colima = std::path::PathBuf::from(&home).join(".colima/default/docker.sock");
            if colima.exists() {
                if let Ok(d) = Docker::connect_with_unix(
                    colima.to_str().unwrap(),
                    120,
                    bollard::API_DEFAULT_VERSION,
                ) {
                    return Ok(d);
                }
            }
        }
        Docker::connect_with_local_defaults()
            .or_else(|_| Docker::connect_with_http_defaults())
            .context("no reachable local Docker-compatible socket")
    }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Bucket {
    Pure,
    SingleNode,
    Cluster3,
    Cluster3Churn,
    Auth,
    Ssl,
    Parity,
}

impl Bucket {
    fn ordered() -> [Bucket; 7] {
        [
            Bucket::Pure,
            Bucket::SingleNode,
            Bucket::Cluster3,
            Bucket::Cluster3Churn,
            Bucket::Auth,
            Bucket::Ssl,
            Bucket::Parity,
        ]
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "pure" => Ok(Bucket::Pure),
            "single_node" => Ok(Bucket::SingleNode),
            "cluster3" => Ok(Bucket::Cluster3),
            "cluster3_churn" => Ok(Bucket::Cluster3Churn),
            "auth" => Ok(Bucket::Auth),
            "ssl" => Ok(Bucket::Ssl),
            "parity" => Ok(Bucket::Parity),
            other => bail!("unknown bucket: {other}"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Bucket::Pure => "pure",
            Bucket::SingleNode => "single_node",
            Bucket::Cluster3 => "cluster3",
            Bucket::Cluster3Churn => "cluster3_churn",
            Bucket::Auth => "auth",
            Bucket::Ssl => "ssl",
            Bucket::Parity => "parity",
        }
    }

    fn is_live(self) -> bool {
        self != Bucket::Pure
    }

    fn requires_ssl_feature(self) -> bool {
        matches!(self, Bucket::Auth | Bucket::Ssl)
    }
}

#[derive(Debug, Deserialize)]
struct TestMatrix {
    suite: Vec<SuiteEntry>,
}

const KNOWN_PROFILES: &[&str] = &[
    "none",
    "single-node",
    "single-node-churn",
    "single-node-auth",
    "single-node-tls",
    "single-node-mtls",
    "cluster-3",
    "cluster-3-churn",
];

impl TestMatrix {
    fn validate(&self) -> Result<()> {
        if self.suite.is_empty() {
            bail!("test matrix is empty");
        }

        for suite in &self.suite {
            let _ = Bucket::parse(&suite.bucket)?;
            if !KNOWN_PROFILES.contains(&suite.profile.as_str()) {
                bail!(
                    "unknown profile {:?} for test {:?} — known profiles: {:?}",
                    suite.profile,
                    suite.test,
                    KNOWN_PROFILES
                );
            }
        }

        Ok(())
    }

    fn bucket(&self, bucket: Bucket) -> Vec<&SuiteEntry> {
        self.suite
            .iter()
            .filter(|suite| suite.bucket == bucket.as_str())
            .collect()
    }
}

#[derive(Debug, Deserialize)]
struct SuiteEntry {
    test: String,
    bucket: String,
    profile: String,
    scope: String,
    #[serde(default)]
    features: Vec<String>,
    #[serde(default)]
    serial: bool,
    #[serde(default)]
    exact: Option<String>,
}

impl SuiteEntry {
    fn live_profile(&self) -> Option<String> {
        if self.profile == "none" {
            None
        } else {
            Some(self.profile.clone())
        }
    }

    fn is_shared_scope(&self) -> bool {
        self.scope == "cargo_session"
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct SuiteGroupKey {
    test: String,
    profile: String,
    scope: String,
    features: Vec<String>,
    serial: bool,
}

impl SuiteGroupKey {
    fn from(suite: &SuiteEntry) -> Self {
        Self {
            test: suite.test.clone(),
            profile: suite.profile.clone(),
            scope: suite.scope.clone(),
            features: suite.features.clone(),
            serial: suite.serial,
        }
    }
}
