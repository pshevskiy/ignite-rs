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

/// Migrated from Apache Ignite `ClusterGroupClusterRestartTest.testGroupNodesAfterClusterRestart`:
/// Java source: org.apache.ignite.internal.client.thin.ClusterGroupClusterRestartTest
#[tokio::test]
async fn should_refresh_cluster_groups_after_cluster_restart() {
    let nodes = [
        (MockUuid::new(9101, 1), 0, false),
        (MockUuid::new(9102, 2), 1, false),
        (MockUuid::new(9103, 3), 2, true),
    ];

    let server = spawn_mock_thin_server(MockThinServerConfig {
        opcode_responses: Some(opcode_responses(vec![
            (
                OP_CLUSTER_GROUP_GET_NODE_IDS,
                vec![
                    MockResponse::success(encode_node_ids_response(true, 1, &nodes)),
                    MockResponse::success(encode_node_ids_response(true, 2, &nodes)),
                    MockResponse::success(encode_node_ids_response(false, 2, &nodes)),
                    MockResponse::success(encode_node_ids_response(false, 2, &nodes)),
                    MockResponse::success(encode_node_ids_response(false, 2, &nodes)),
                    MockResponse::success(encode_node_ids_response(false, 2, &nodes)),
                    MockResponse::success(encode_node_ids_response(false, 2, &nodes)),
                    MockResponse::success(encode_node_ids_response(false, 2, &nodes)),
                ],
            ),
            (
                OP_CLUSTER_GROUP_GET_NODE_INFO,
                vec![
                    MockResponse::success(encode_node_info_payload(&to_mock_node_infos(&nodes))),
                    MockResponse::success(encode_node_info_payload(&to_mock_node_infos(&nodes))),
                ],
            ),
        ])),
        ..Default::default()
    });

    let mut conf = ClientConfig::new(server.addr());
    conf.partition_awareness_enabled = false;
    let client = new_client(conf).await.unwrap();

    let dflt = client.cluster().group();
    let srv = client.cluster().for_servers();
    let cli = client.cluster().for_clients();
    let attr = client
        .cluster()
        .for_attribute("IDX_ATTR", Some(SqlValue::Int(0)));

    assert_eq!(dflt.node_ids().await.unwrap().len(), 3);
    assert_eq!(srv.node_ids().await.unwrap().len(), 2);
    assert_eq!(cli.node_ids().await.unwrap(), vec![nodes[2].0.as_string()]);
    assert_eq!(attr.node_ids().await.unwrap(), vec![nodes[0].0.as_string()]);

    assert_eq!(dflt.node_ids().await.unwrap().len(), 3);
    assert_eq!(srv.node_ids().await.unwrap().len(), 2);
    assert_eq!(cli.node_ids().await.unwrap(), vec![nodes[2].0.as_string()]);
    assert_eq!(attr.node_ids().await.unwrap(), vec![nodes[0].0.as_string()]);
}

fn encode_node_ids_response(
    changed: bool,
    topology_version: i64,
    nodes: &[(MockUuid, i32, bool)],
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(changed as u8);
    if changed {
        payload.extend_from_slice(&topology_version.to_le_bytes());
        payload.extend_from_slice(&(nodes.len() as i32).to_le_bytes());
        for (node, _, _) in nodes {
            payload.extend_from_slice(&node.most.to_le_bytes());
            payload.extend_from_slice(&node.least.to_le_bytes());
        }
    }
    payload
}

fn to_mock_node_infos(nodes: &[(MockUuid, i32, bool)]) -> Vec<MockNodeInfo> {
    nodes
        .iter()
        .map(|(uuid, idx_attr, is_client)| {
            MockNodeInfo {
                uuid: *uuid,
                attributes: Vec::new(),
                addresses: vec!["127.0.0.1".to_string()],
                host_names: vec![if *is_client { "client" } else { "server" }.to_string()],
                order: *idx_attr as i64 + 1,
                is_local: false,
                is_daemon: false,
                is_client: *is_client,
                consistent_id: format!("node-{}", idx_attr),
            }
            .with_typed_attribute("IDX_ATTR", idx_attr)
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
