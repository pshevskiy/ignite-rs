use crate::api::OpCode;
use crate::error::{IgniteError, IgniteResult};
use crate::exec::TokioExec;
use crate::protocol::{
    read_bool, read_i32, read_i64, read_string, read_u8, write_bool, write_i32, write_i64,
    write_string,
};
use crate::query::sql::{read_sql_value, SqlValue};
use crate::{ReadableReq, WriteableReq};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterState {
    Inactive = 0,
    Active = 1,
    ActiveReadOnly = 2,
}

impl ClusterState {
    fn from_wire(value: u8) -> IgniteResult<Self> {
        match value {
            0 => Ok(Self::Inactive),
            1 => Ok(Self::Active),
            2 => Ok(Self::ActiveReadOnly),
            _ => Err(IgniteError::from(
                format!("Unexpected cluster state ordinal {}", value).as_str(),
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNodeVersion {
    pub major: u8,
    pub minor: u8,
    pub maintenance: u8,
    pub stage: String,
    pub revision_timestamp: i64,
    pub revision_hash: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterNode {
    pub id: String,
    pub attributes: HashMap<String, SqlValue>,
    pub addresses: Vec<String>,
    pub host_names: Vec<String>,
    pub order: i64,
    pub is_local: bool,
    pub is_daemon: bool,
    pub is_client: bool,
    pub consistent_id: SqlValue,
    pub version: ClusterNodeVersion,
}

#[derive(Clone)]
pub struct Cluster {
    core: Arc<ClusterCore>,
}

#[derive(Clone)]
pub struct ClusterGroup {
    core: Arc<ClusterCore>,
    selectors: Vec<Selector>,
}

#[derive(Debug, Default)]
struct ClusterCache {
    topology_version: Option<i64>,
    node_ids: Vec<String>,
    nodes: HashMap<String, ClusterNode>,
}

struct ClusterCore {
    exec: TokioExec,
    cache: Mutex<ClusterCache>,
}

#[derive(Clone, Debug, PartialEq)]
enum Selector {
    NodeIds(Vec<String>),
    Others(Vec<String>),
    Servers,
    Clients,
    Attribute(String, Option<SqlValue>),
    Hosts(Vec<String>),
    Random,
    Oldest,
    Youngest,
}

impl Cluster {
    pub(crate) fn new(exec: TokioExec) -> Self {
        Self {
            core: Arc::new(ClusterCore {
                exec,
                cache: Mutex::new(ClusterCache::default()),
            }),
        }
    }

    pub async fn state(&self) -> IgniteResult<ClusterState> {
        self.core
            .exec
            .send_and_read(OpCode::ClusterGetState, EmptyRequest)
            .await
    }

    pub async fn set_state(&self, state: ClusterState) -> IgniteResult<()> {
        self.set_state_with_force(state, true).await
    }

    pub async fn set_state_with_force(
        &self,
        state: ClusterState,
        force_deactivation: bool,
    ) -> IgniteResult<()> {
        self.core
            .exec
            .send(
                OpCode::ClusterChangeState,
                ClusterChangeStateRequest {
                    state,
                    force_deactivation,
                },
            )
            .await
    }

    pub async fn enable_wal(&self, cache_name: &str) -> IgniteResult<bool> {
        self.change_wal_state(cache_name, true).await
    }

    pub async fn disable_wal(&self, cache_name: &str) -> IgniteResult<bool> {
        self.change_wal_state(cache_name, false).await
    }

    pub async fn is_wal_enabled(&self, cache_name: &str) -> IgniteResult<bool> {
        let response: BoolResponse = self
            .core
            .exec
            .send_and_read(
                OpCode::ClusterGetWalState,
                ClusterWalStateRequest {
                    cache_name: cache_name.to_string(),
                    enable: None,
                },
            )
            .await?;
        Ok(response.value)
    }

    pub fn group(&self) -> ClusterGroup {
        ClusterGroup::new(self.core.clone(), Vec::new())
    }

    pub fn for_servers(&self) -> ClusterGroup {
        self.group().for_servers()
    }

    pub fn for_clients(&self) -> ClusterGroup {
        self.group().for_clients()
    }

    pub fn for_node_ids<I, S>(&self, node_ids: I) -> ClusterGroup
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.group().for_node_ids(node_ids)
    }

    pub fn for_node_id(&self, node_id: impl Into<String>) -> ClusterGroup {
        self.group().for_node_id(node_id)
    }

    pub fn for_attribute(&self, name: &str, value: Option<SqlValue>) -> ClusterGroup {
        self.group().for_attribute(name, value)
    }

    pub fn for_host(&self, host: &str) -> ClusterGroup {
        self.group().for_host(host)
    }

    pub fn for_random(&self) -> ClusterGroup {
        self.group().for_random()
    }

    pub fn for_oldest(&self) -> ClusterGroup {
        self.group().for_oldest()
    }

    pub fn for_youngest(&self) -> ClusterGroup {
        self.group().for_youngest()
    }

    pub async fn nodes(&self) -> IgniteResult<Vec<ClusterNode>> {
        self.group().nodes().await
    }

    pub async fn node(&self, node_id: &str) -> IgniteResult<Option<ClusterNode>> {
        self.group().node(node_id).await
    }

    async fn change_wal_state(&self, cache_name: &str, enable: bool) -> IgniteResult<bool> {
        let response: BoolResponse = self
            .core
            .exec
            .send_and_read(
                OpCode::ClusterChangeWalState,
                ClusterWalStateRequest {
                    cache_name: cache_name.to_string(),
                    enable: Some(enable),
                },
            )
            .await?;
        Ok(response.value)
    }
}

impl ClusterGroup {
    fn new(core: Arc<ClusterCore>, selectors: Vec<Selector>) -> Self {
        Self { core, selectors }
    }

    pub(crate) fn default_servers(exec: TokioExec) -> Self {
        Cluster::new(exec).for_servers()
    }

    pub fn for_servers(&self) -> Self {
        self.with_selector(Selector::Servers)
    }

    pub fn for_clients(&self) -> Self {
        self.with_selector(Selector::Clients)
    }

    pub fn for_node_ids<I, S>(&self, node_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.with_selector(Selector::NodeIds(
            node_ids.into_iter().map(Into::into).collect(),
        ))
    }

    pub fn for_node_id(&self, node_id: impl Into<String>) -> Self {
        self.with_selector(Selector::NodeIds(vec![node_id.into()]))
    }

    pub fn for_nodes(&self, nodes: &[ClusterNode]) -> Self {
        self.for_node_ids(nodes.iter().map(|node| node.id.clone()).collect::<Vec<_>>())
    }

    pub fn for_node(&self, node: &ClusterNode) -> Self {
        self.for_node_id(node.id.clone())
    }

    pub fn for_others(&self, nodes: &[ClusterNode]) -> Self {
        self.with_selector(Selector::Others(
            nodes.iter().map(|node| node.id.clone()).collect(),
        ))
    }

    pub fn for_attribute(&self, name: &str, value: Option<SqlValue>) -> Self {
        self.with_selector(Selector::Attribute(name.to_string(), value))
    }

    pub fn for_host(&self, host: &str) -> Self {
        self.with_selector(Selector::Hosts(vec![host.to_string()]))
    }

    pub fn for_hosts<I, S>(&self, hosts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.with_selector(Selector::Hosts(hosts.into_iter().map(Into::into).collect()))
    }

    pub fn for_random(&self) -> Self {
        self.with_selector(Selector::Random)
    }

    pub fn for_oldest(&self) -> Self {
        self.with_selector(Selector::Oldest)
    }

    pub fn for_youngest(&self) -> Self {
        self.with_selector(Selector::Youngest)
    }

    pub async fn node_ids(&self) -> IgniteResult<Vec<String>> {
        Ok(self
            .nodes()
            .await?
            .into_iter()
            .map(|node| node.id)
            .collect())
    }

    pub async fn node(&self, node_id: &str) -> IgniteResult<Option<ClusterNode>> {
        Ok(self
            .nodes()
            .await?
            .into_iter()
            .find(|node| node.id == node_id))
    }

    pub async fn nodes(&self) -> IgniteResult<Vec<ClusterNode>> {
        let node_ids = self.core.request_node_ids().await?;
        let nodes = self.core.request_nodes_by_ids(&node_ids).await?;
        Ok(apply_selectors(nodes, &self.selectors))
    }

    fn with_selector(&self, selector: Selector) -> Self {
        let mut selectors = self.selectors.clone();
        selectors.push(selector);
        Self {
            core: self.core.clone(),
            selectors,
        }
    }
}

impl ClusterCore {
    async fn request_node_ids(&self) -> IgniteResult<Vec<String>> {
        let cached_topology_version = self.cache.lock().await.topology_version.unwrap_or(0);
        let response: NodeIdsResponse = self
            .exec
            .send_and_read(
                OpCode::ClusterGroupGetNodeIds,
                NodeIdsRequest {
                    topology_version: cached_topology_version,
                },
            )
            .await?;

        let mut cache = self.cache.lock().await;
        if response.changed {
            cache.topology_version = Some(response.topology_version.unwrap_or(0));
            cache.node_ids = response.node_ids.clone();
            let valid_node_ids = cache.node_ids.clone();
            cache
                .nodes
                .retain(|node_id, _| valid_node_ids.contains(node_id));
        }

        Ok(cache.node_ids.clone())
    }

    async fn request_nodes_by_ids(&self, node_ids: &[String]) -> IgniteResult<Vec<ClusterNode>> {
        let missing_ids = {
            let cache = self.cache.lock().await;
            node_ids
                .iter()
                .filter(|node_id| !cache.nodes.contains_key((*node_id).as_str()))
                .cloned()
                .collect::<Vec<_>>()
        };

        if !missing_ids.is_empty() {
            let response: NodeInfoResponse = self
                .exec
                .send_and_read(
                    OpCode::ClusterGroupGetNodeInfo,
                    NodeInfoRequest {
                        node_ids: missing_ids,
                    },
                )
                .await?;

            let mut cache = self.cache.lock().await;
            for node in response.nodes {
                cache.nodes.insert(node.id.clone(), node);
            }
        }

        let cache = self.cache.lock().await;
        Ok(node_ids
            .iter()
            .filter_map(|node_id| cache.nodes.get(node_id).cloned())
            .collect())
    }
}

pub(crate) fn parse_uuid_parts(uuid: &str) -> IgniteResult<(i64, i64)> {
    let hex = uuid.replace('-', "");
    if hex.len() != 32 {
        return Err(IgniteError::from(
            format!("Invalid UUID string '{}'", uuid).as_str(),
        ));
    }

    let most = u64::from_str_radix(&hex[..16], 16)
        .map_err(|err| IgniteError::from(err.to_string().as_str()))?;
    let least = u64::from_str_radix(&hex[16..], 16)
        .map_err(|err| IgniteError::from(err.to_string().as_str()))?;
    Ok((most as i64, least as i64))
}

fn apply_selectors(mut nodes: Vec<ClusterNode>, selectors: &[Selector]) -> Vec<ClusterNode> {
    for selector in selectors {
        match selector {
            Selector::NodeIds(node_ids) => {
                nodes.retain(|node| node_ids.iter().any(|candidate| candidate == &node.id));
            }
            Selector::Others(node_ids) => {
                nodes.retain(|node| !node_ids.iter().any(|candidate| candidate == &node.id));
            }
            Selector::Servers => nodes.retain(|node| !node.is_client),
            Selector::Clients => nodes.retain(|node| node.is_client),
            Selector::Attribute(name, value) => nodes.retain(|node| match value {
                Some(value) => node
                    .attributes
                    .get(name)
                    .map(|attr| attr == value)
                    .unwrap_or(false),
                None => node.attributes.contains_key(name),
            }),
            Selector::Hosts(hosts) => nodes.retain(|node| {
                node.host_names
                    .iter()
                    .any(|host_name| hosts.iter().any(|candidate| candidate == host_name))
            }),
            Selector::Random => {
                if !nodes.is_empty() {
                    let nanos = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|duration| duration.subsec_nanos() as usize)
                        .unwrap_or(0);
                    let idx = nanos % nodes.len();
                    nodes = vec![nodes.remove(idx)];
                }
            }
            Selector::Oldest => {
                if let Some(node) = nodes.iter().min_by_key(|node| node.order).cloned() {
                    nodes = vec![node];
                }
            }
            Selector::Youngest => {
                if let Some(node) = nodes.iter().max_by_key(|node| node.order).cloned() {
                    nodes = vec![node];
                }
            }
        }
    }

    nodes
}

struct EmptyRequest;

impl WriteableReq for EmptyRequest {
    fn write(&self, _writer: &mut dyn Write) -> io::Result<()> {
        Ok(())
    }

    fn size(&self) -> usize {
        0
    }
}

struct ClusterChangeStateRequest {
    state: ClusterState,
    force_deactivation: bool,
}

impl WriteableReq for ClusterChangeStateRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        crate::protocol::write_u8(writer, self.state as u8)?;
        write_bool(writer, self.force_deactivation)?;
        Ok(())
    }

    fn size(&self) -> usize {
        2
    }
}

struct ClusterWalStateRequest {
    cache_name: String,
    enable: Option<bool>,
}

impl WriteableReq for ClusterWalStateRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_string(writer, &self.cache_name)?;
        if let Some(enable) = self.enable {
            write_bool(writer, enable)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        4 + self.cache_name.len() + self.enable.map(|_| 1).unwrap_or(0)
    }
}

struct NodeIdsRequest {
    topology_version: i64,
}

impl WriteableReq for NodeIdsRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_i64(writer, self.topology_version)?;
        write_i32(writer, 0)?;
        Ok(())
    }

    fn size(&self) -> usize {
        8 + 4
    }
}

struct NodeIdsResponse {
    changed: bool,
    topology_version: Option<i64>,
    node_ids: Vec<String>,
}

impl ReadableReq for NodeIdsResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let changed = read_bool(reader).map_err(IgniteError::from)?;
        if !changed {
            return Ok(Self {
                changed,
                topology_version: None,
                node_ids: Vec::new(),
            });
        }

        let topology_version = read_i64(reader).map_err(IgniteError::from)?;
        let count = read_i32(reader).map_err(IgniteError::from)?;
        if count < 0 {
            return Err(IgniteError::from("negative cluster node id count"));
        }

        let mut node_ids = Vec::with_capacity(count as usize);
        for _ in 0..count {
            node_ids.push(crate::connection_async::read_uuid_string(reader)?);
        }

        Ok(Self {
            changed,
            topology_version: Some(topology_version),
            node_ids,
        })
    }
}

struct NodeInfoRequest {
    node_ids: Vec<String>,
}

impl WriteableReq for NodeInfoRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_i32(writer, self.node_ids.len() as i32)?;
        for node_id in &self.node_ids {
            let (most, least) = parse_uuid_parts(node_id)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
            write_i64(writer, most)?;
            write_i64(writer, least)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        4 + self.node_ids.len() * 16
    }
}

struct NodeInfoResponse {
    nodes: Vec<ClusterNode>,
}

impl ReadableReq for NodeInfoResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let count = read_i32(reader).map_err(IgniteError::from)?;
        if count < 0 {
            return Err(IgniteError::from("negative cluster node info count"));
        }

        let mut nodes = Vec::with_capacity(count as usize);
        for _ in 0..count {
            nodes.push(read_cluster_node(reader)?);
        }

        Ok(Self { nodes })
    }
}

struct BoolResponse {
    value: bool,
}

impl ReadableReq for BoolResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        Ok(Self {
            value: read_bool(reader).map_err(IgniteError::from)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{ClusterChangeStateRequest, ClusterState, ClusterWalStateRequest};
    use crate::WriteableReq;

    fn encode_request(req: &impl WriteableReq) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(req.size());
        req.write(&mut bytes).unwrap();
        bytes
    }

    #[test]
    fn should_encode_cluster_change_state_request() {
        let bytes = encode_request(&ClusterChangeStateRequest {
            state: ClusterState::ActiveReadOnly,
            force_deactivation: false,
        });

        assert_eq!(bytes, vec![ClusterState::ActiveReadOnly as u8, 0]);
    }

    #[test]
    fn should_encode_cluster_wal_state_change_request() {
        let bytes = encode_request(&ClusterWalStateRequest {
            cache_name: "cacheA".to_string(),
            enable: Some(true),
        });

        assert_eq!(
            bytes,
            vec![6, 0, 0, 0, b'c', b'a', b'c', b'h', b'e', b'A', 1,]
        );
    }

    #[test]
    fn should_encode_cluster_wal_state_read_request_without_flag() {
        let bytes = encode_request(&ClusterWalStateRequest {
            cache_name: "cacheA".to_string(),
            enable: None,
        });

        assert_eq!(bytes, vec![6, 0, 0, 0, b'c', b'a', b'c', b'h', b'e', b'A']);
    }
}

impl ReadableReq for ClusterState {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        ClusterState::from_wire(read_u8(reader).map_err(IgniteError::from)?)
    }
}

fn read_cluster_node(reader: &mut impl Read) -> IgniteResult<ClusterNode> {
    let id = crate::connection_async::read_uuid_string(reader)?;
    let attr_count = read_i32(reader).map_err(IgniteError::from)?;
    if attr_count < 0 {
        return Err(IgniteError::from("negative cluster node attribute count"));
    }

    let mut attributes = HashMap::with_capacity(attr_count as usize);
    for _ in 0..attr_count {
        let name = read_string(reader).map_err(IgniteError::from)?;
        let value = read_sql_value(reader)?;
        attributes.insert(name, value);
    }

    Ok(ClusterNode {
        id,
        attributes,
        addresses: read_string_collection(reader)?,
        host_names: read_string_collection(reader)?,
        order: read_i64(reader).map_err(IgniteError::from)?,
        is_local: read_bool(reader).map_err(IgniteError::from)?,
        is_daemon: read_bool(reader).map_err(IgniteError::from)?,
        is_client: read_bool(reader).map_err(IgniteError::from)?,
        consistent_id: read_sql_value(reader)?,
        version: ClusterNodeVersion {
            major: read_u8(reader).map_err(IgniteError::from)?,
            minor: read_u8(reader).map_err(IgniteError::from)?,
            maintenance: read_u8(reader).map_err(IgniteError::from)?,
            stage: read_string(reader).map_err(IgniteError::from)?,
            revision_timestamp: read_i64(reader).map_err(IgniteError::from)?,
            revision_hash: read_byte_array(reader)?,
        },
    })
}

fn read_string_collection(reader: &mut impl Read) -> IgniteResult<Vec<String>> {
    let count = read_i32(reader).map_err(IgniteError::from)?;
    if count < 0 {
        return Err(IgniteError::from("negative string collection count"));
    }

    let mut values = Vec::with_capacity(count as usize);
    for _ in 0..count {
        values.push(read_string(reader).map_err(IgniteError::from)?);
    }
    Ok(values)
}

fn read_byte_array(reader: &mut impl Read) -> IgniteResult<Vec<u8>> {
    let count = read_i32(reader).map_err(IgniteError::from)?;
    if count < 0 {
        return Ok(Vec::new());
    }
    let mut buf = vec![0u8; count as usize];
    reader.read_exact(&mut buf).map_err(IgniteError::from)?;
    Ok(buf)
}
