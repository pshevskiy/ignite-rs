#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    encode_node_info_payload, spawn_mock_thin_server, MockNodeInfo, MockResponse,
    MockThinServerConfig, MockUuid,
};
use ignite_rs::query::SqlValue;
use ignite_rs::{new_client, ClientConfig};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

const OP_CLUSTER_GROUP_GET_NODE_IDS: i16 = 5100;
const OP_CLUSTER_GROUP_GET_NODE_INFO: i16 = 5101;

#[derive(Clone)]
struct NodeSpec {
    id: MockUuid,
    attr_name: &'static str,
    attr_value: i32,
    addresses: Vec<&'static str>,
    host_names: Vec<&'static str>,
    order: i64,
    is_client: bool,
    consistent_id: &'static str,
}

/// Migrated from Apache Ignite `ClusterGroupTest.testForServers`, `testForClients`, `testForAttribute`, and `testForHost`:
/// Java source: org.apache.ignite.internal.client.thin.ClusterGroupTest
#[tokio::test]
async fn should_filter_servers_clients_attributes_and_hosts() {
    let nodes = sample_nodes();
    let server = spawn_mock_thin_server(MockThinServerConfig {
        opcode_responses: Some(opcode_responses(vec![
            (
                OP_CLUSTER_GROUP_GET_NODE_IDS,
                vec![
                    MockResponse::success(encode_node_ids_response(true, 1, &nodes)),
                    MockResponse::success(encode_node_ids_response(false, 1, &nodes)),
                    MockResponse::success(encode_node_ids_response(false, 1, &nodes)),
                    MockResponse::success(encode_node_ids_response(false, 1, &nodes)),
                    MockResponse::success(encode_node_ids_response(false, 1, &nodes)),
                ],
            ),
            (
                OP_CLUSTER_GROUP_GET_NODE_INFO,
                vec![MockResponse::success(encode_node_info_payload(
                    &to_mock_node_infos(&nodes),
                ))],
            ),
        ])),
        ..Default::default()
    });

    let mut conf = ClientConfig::new(server.addr());
    conf.partition_awareness_enabled = false;
    let client = new_client(conf).await.unwrap();
    let cluster = client.cluster();

    let all = cluster.nodes().await.unwrap();
    assert_eq!(all.len(), 3);

    let servers = cluster.for_servers().node_ids().await.unwrap();
    assert_eq!(
        servers,
        vec![nodes[0].id.as_string(), nodes[1].id.as_string()]
    );

    let clients = cluster.for_clients().node_ids().await.unwrap();
    assert_eq!(clients, vec![nodes[2].id.as_string()]);

    let attr = cluster
        .for_attribute("IDX_ATTR", Some(SqlValue::Int(0)))
        .node_ids()
        .await
        .unwrap();
    assert_eq!(attr, vec![nodes[0].id.as_string()]);

    let host = cluster.for_host("client-host").node_ids().await.unwrap();
    assert_eq!(host, vec![nodes[2].id.as_string()]);
}

/// Migrated from Apache Ignite `ClusterGroupTest.testForNodeIds`, `testForOldest`, `testForYoungest`, and `testForRandom`:
/// Java source: org.apache.ignite.internal.client.thin.ClusterGroupTest
#[tokio::test]
async fn should_select_specific_oldest_youngest_and_random_nodes() {
    let nodes = sample_nodes();
    let server = spawn_mock_thin_server(MockThinServerConfig {
        opcode_responses: Some(opcode_responses(vec![
            (
                OP_CLUSTER_GROUP_GET_NODE_IDS,
                vec![
                    MockResponse::success(encode_node_ids_response(true, 1, &nodes)),
                    MockResponse::success(encode_node_ids_response(false, 1, &nodes)),
                    MockResponse::success(encode_node_ids_response(false, 1, &nodes)),
                    MockResponse::success(encode_node_ids_response(false, 1, &nodes)),
                    MockResponse::success(encode_node_ids_response(false, 1, &nodes)),
                ],
            ),
            (
                OP_CLUSTER_GROUP_GET_NODE_INFO,
                vec![MockResponse::success(encode_node_info_payload(
                    &to_mock_node_infos(&nodes),
                ))],
            ),
        ])),
        ..Default::default()
    });

    let mut conf = ClientConfig::new(server.addr());
    conf.partition_awareness_enabled = false;
    let client = new_client(conf).await.unwrap();
    let cluster = client.cluster();

    let explicit = cluster
        .for_node_ids([nodes[1].id.as_string(), nodes[2].id.as_string()])
        .node_ids()
        .await
        .unwrap();
    assert_eq!(
        explicit,
        vec![nodes[1].id.as_string(), nodes[2].id.as_string()]
    );

    let oldest = cluster.for_oldest().node_ids().await.unwrap();
    assert_eq!(oldest, vec![nodes[0].id.as_string()]);

    let youngest = cluster.for_youngest().node_ids().await.unwrap();
    assert_eq!(youngest, vec![nodes[1].id.as_string()]);

    let random = cluster.for_random().nodes().await.unwrap();
    assert_eq!(random.len(), 1);
    assert!(nodes.iter().any(|node| node.id.as_string() == random[0].id));
}

fn sample_nodes() -> Vec<NodeSpec> {
    vec![
        NodeSpec {
            id: MockUuid::new(1001, 1),
            attr_name: "IDX_ATTR",
            attr_value: 0,
            addresses: vec!["127.0.0.1"],
            host_names: vec!["server-a"],
            order: 1,
            is_client: false,
            consistent_id: "srv-0",
        },
        NodeSpec {
            id: MockUuid::new(1002, 2),
            attr_name: "IDX_ATTR",
            attr_value: 1,
            addresses: vec!["127.0.0.2"],
            host_names: vec!["server-b"],
            order: 9,
            is_client: false,
            consistent_id: "srv-1",
        },
        NodeSpec {
            id: MockUuid::new(1003, 3),
            attr_name: "IDX_ATTR",
            attr_value: 2,
            addresses: vec!["127.0.0.3"],
            host_names: vec!["client-host"],
            order: 5,
            is_client: true,
            consistent_id: "cli-0",
        },
    ]
}

fn encode_node_ids_response(changed: bool, topology_version: i64, nodes: &[NodeSpec]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(changed as u8);
    if changed {
        payload.extend_from_slice(&topology_version.to_le_bytes());
        payload.extend_from_slice(&(nodes.len() as i32).to_le_bytes());
        for node in nodes {
            payload.extend_from_slice(&node.id.most.to_le_bytes());
            payload.extend_from_slice(&node.id.least.to_le_bytes());
        }
    }
    payload
}

fn to_mock_node_infos(nodes: &[NodeSpec]) -> Vec<MockNodeInfo> {
    nodes
        .iter()
        .map(|node| {
            MockNodeInfo {
                uuid: node.id,
                attributes: Vec::new(),
                addresses: node.addresses.iter().map(|s| s.to_string()).collect(),
                host_names: node.host_names.iter().map(|s| s.to_string()).collect(),
                order: node.order,
                is_local: false,
                is_daemon: false,
                is_client: node.is_client,
                consistent_id: node.consistent_id.to_string(),
            }
            .with_typed_attribute(node.attr_name, &node.attr_value)
        })
        .collect()
}

fn opcode_responses(
    entries: Vec<(i16, Vec<MockResponse>)>,
) -> Arc<Mutex<HashMap<i16, VecDeque<MockResponse>>>> {
    Arc::new(Mutex::new(
        entries
            .into_iter()
            .map(|(op, responses)| (op, VecDeque::from(responses)))
            .collect(),
    ))
}
