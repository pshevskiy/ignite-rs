use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use arc_swap::ArcSwap;
use tokio::sync::RwLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TopologyVersion {
    pub(crate) major: i64,
    pub(crate) minor: i32,
}

impl TopologyVersion {
    pub(crate) const fn new(major: i64, minor: i32) -> Self {
        Self { major, minor }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DiscoveredNode {
    pub(crate) node_id: String,
    pub(crate) endpoints: Vec<String>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, Default)]
pub(crate) struct TopologySnapshot {
    pub(crate) seed_endpoints: Vec<String>,
    pub(crate) discovered_endpoints: Vec<String>,
    pub(crate) endpoints: Vec<String>,
    pub(crate) active_endpoint: Option<String>,
    pub(crate) node_ids: Vec<String>,
    pub(crate) current_dc_node_ids: Vec<String>,
    pub(crate) topology_version: Option<TopologyVersion>,
}

#[derive(Debug)]
struct TopologyState {
    seed_endpoints: Vec<String>,
    discovered_nodes: BTreeMap<String, Vec<String>>,
    active_endpoint: Option<String>,
    node_ids: Vec<String>,
    current_dc_node_ids: Vec<String>,
    topology_version: Option<TopologyVersion>,
}

#[derive(Debug)]
pub(crate) struct TopologyCache {
    state: RwLock<TopologyState>,
    /// PFND-001: pre-computed merged endpoints. Invalidated on seed /
    /// discovery mutations; rebuilt under the write lock. Hot-path readers
    /// (one per request) clone this Arc instead of re-running dedupe +
    /// HashSet + String cloning.
    cached_endpoints: ArcSwap<Vec<String>>,
}

#[allow(dead_code)]
impl TopologyCache {
    pub(crate) fn new(seed_endpoints: Vec<String>) -> Self {
        let seed_endpoints = dedupe(seed_endpoints);
        let initial_cache = Arc::new(seed_endpoints.clone());
        Self {
            state: RwLock::new(TopologyState {
                seed_endpoints,
                discovered_nodes: BTreeMap::new(),
                active_endpoint: None,
                node_ids: Vec::new(),
                current_dc_node_ids: Vec::new(),
                topology_version: None,
            }),
            cached_endpoints: ArcSwap::new(initial_cache),
        }
    }

    /// Returns a shared view of the merged seed + discovered endpoints.
    ///
    /// PFND-001: lock-free on the hot path. Callers typically iterate the
    /// result and optionally clone individual strings.
    pub(crate) fn endpoints_arc(&self) -> Arc<Vec<String>> {
        self.cached_endpoints.load_full()
    }

    pub(crate) async fn endpoints(&self) -> Vec<String> {
        // Back-compat shim: some code still wants an owned Vec<String>.
        (*self.endpoints_arc()).clone()
    }

    fn rebuild_cache(state: &TopologyState) -> Arc<Vec<String>> {
        Arc::new(merged_endpoints(
            &state.seed_endpoints,
            &flatten_discovered_nodes(&state.discovered_nodes),
        ))
    }

    fn refresh_cache_from(&self, state: &TopologyState) {
        self.cached_endpoints.store(Self::rebuild_cache(state));
    }

    pub(crate) async fn replace_seed_endpoints(&self, seed_endpoints: Vec<String>) -> Vec<String> {
        let mut state = self.state.write().await;
        let next = dedupe(seed_endpoints);
        let removed = state
            .seed_endpoints
            .iter()
            .filter(|endpoint| !next.iter().any(|candidate| candidate == *endpoint))
            .cloned()
            .collect();
        state.seed_endpoints = next;
        self.refresh_cache_from(&state);
        removed
    }

    pub(crate) async fn endpoint_count(&self) -> usize {
        self.endpoints().await.len()
    }

    pub(crate) async fn next_index_after(&self, address: &str) -> usize {
        let endpoints = self.endpoints().await;
        if endpoints.is_empty() {
            return 0;
        }

        endpoints
            .iter()
            .position(|candidate| candidate == address)
            .map(|index| (index + 1) % endpoints.len())
            .unwrap_or(0)
    }

    pub(crate) async fn mark_active(&self, address: &str) {
        let mut state = self.state.write().await;
        state.active_endpoint = Some(address.to_string());
    }

    pub(crate) async fn record_node(&self, node_id: impl Into<String>) {
        let mut state = self.state.write().await;
        let node_id = node_id.into();
        if !state.node_ids.iter().any(|existing| existing == &node_id) {
            state.node_ids.push(node_id);
        }
    }

    pub(crate) async fn record_node_endpoint(
        &self,
        node_id: impl Into<String>,
        endpoint: impl Into<String>,
    ) {
        let mut state = self.state.write().await;
        let node_id = node_id.into();
        let endpoint = endpoint.into();

        if !state.node_ids.iter().any(|existing| existing == &node_id) {
            state.node_ids.push(node_id.clone());
        }

        let endpoints = state.discovered_nodes.entry(node_id).or_default();
        if !endpoints.iter().any(|existing| existing == &endpoint) {
            endpoints.push(endpoint);
        }
        self.refresh_cache_from(&state);
    }

    pub(crate) async fn topology_version(&self) -> Option<TopologyVersion> {
        self.state.read().await.topology_version
    }

    pub(crate) async fn set_topology_version(&self, version: TopologyVersion) {
        self.state.write().await.topology_version = Some(version);
    }

    pub(crate) async fn set_current_dc_nodes(&self, node_ids: Vec<String>) {
        self.state.write().await.current_dc_node_ids = dedupe(node_ids);
    }

    pub(crate) async fn clear_current_dc_nodes(&self) {
        self.state.write().await.current_dc_node_ids.clear();
    }

    pub(crate) async fn current_dc_nodes(&self) -> Vec<String> {
        self.state.read().await.current_dc_node_ids.clone()
    }

    pub(crate) async fn needs_refresh(&self, version: TopologyVersion) -> bool {
        self.state
            .read()
            .await
            .topology_version
            .map(|current| version > current)
            .unwrap_or(true)
    }

    pub(crate) async fn apply_discovery_update(
        &self,
        version: TopologyVersion,
        added_nodes: Vec<DiscoveredNode>,
        removed_node_ids: &[String],
    ) {
        let mut state = self.state.write().await;

        for node in added_nodes {
            if !state
                .node_ids
                .iter()
                .any(|existing| existing == &node.node_id)
            {
                state.node_ids.push(node.node_id.clone());
            }
            state
                .discovered_nodes
                .insert(node.node_id, dedupe(node.endpoints));
        }

        for node_id in removed_node_ids {
            state.discovered_nodes.remove(node_id);
            state.node_ids.retain(|existing| existing != node_id);
            state
                .current_dc_node_ids
                .retain(|existing| existing != node_id);
        }

        state.topology_version = Some(version);
        self.refresh_cache_from(&state);
    }

    pub(crate) async fn snapshot(&self) -> TopologySnapshot {
        let state = self.state.read().await;
        let discovered_endpoints = flatten_discovered_nodes(&state.discovered_nodes);
        TopologySnapshot {
            seed_endpoints: state.seed_endpoints.clone(),
            discovered_endpoints: discovered_endpoints.clone(),
            endpoints: merged_endpoints(&state.seed_endpoints, &discovered_endpoints),
            active_endpoint: state.active_endpoint.clone(),
            node_ids: state.node_ids.clone(),
            current_dc_node_ids: state.current_dc_node_ids.clone(),
            topology_version: state.topology_version,
        }
    }

    pub(crate) async fn endpoints_for_node(&self, node_id: &str) -> Vec<String> {
        self.state
            .read()
            .await
            .discovered_nodes
            .get(node_id)
            .cloned()
            .unwrap_or_default()
    }
}

fn flatten_discovered_nodes(discovered_nodes: &BTreeMap<String, Vec<String>>) -> Vec<String> {
    let mut flattened = Vec::new();
    for endpoints in discovered_nodes.values() {
        flattened.extend(endpoints.iter().cloned());
    }
    dedupe(flattened)
}

fn dedupe(endpoints: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    endpoints
        .into_iter()
        .filter(|endpoint| seen.insert(endpoint.clone()))
        .collect()
}

fn merged_endpoints(seed_endpoints: &[String], discovered_endpoints: &[String]) -> Vec<String> {
    let mut merged = seed_endpoints.to_vec();
    merged.extend(discovered_endpoints.iter().cloned());
    dedupe(merged)
}
