#![cfg(not(feature = "ssl"))]

mod common;

use common::{spawn_mock_thin_server, MockResponse, MockThinServerConfig, MockUuid};
use ignite_rs::query::SqlValue;
use ignite_rs::{new_client, ClientConfig, WritableType};
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
                    MockResponse::success(encode_node_info_response(&nodes)),
                    MockResponse::success(encode_node_info_response(&nodes)),
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

fn encode_node_info_response(nodes: &[(MockUuid, i32, bool)]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(nodes.len() as i32).to_le_bytes());
    for (node, idx_attr, is_client) in nodes {
        payload.extend_from_slice(&node.most.to_le_bytes());
        payload.extend_from_slice(&node.least.to_le_bytes());
        payload.extend_from_slice(&1i32.to_le_bytes());
        write_raw_string(&mut payload, "IDX_ATTR");
        idx_attr.write(&mut payload).unwrap();
        payload.extend_from_slice(&1i32.to_le_bytes());
        write_raw_string(&mut payload, "127.0.0.1");
        payload.extend_from_slice(&1i32.to_le_bytes());
        write_raw_string(&mut payload, if *is_client { "client" } else { "server" });
        payload.extend_from_slice(&(*idx_attr as i64 + 1).to_le_bytes());
        payload.push(0);
        payload.push(0);
        payload.push(*is_client as u8);
        format!("node-{}", idx_attr).write(&mut payload).unwrap();
        payload.push(2);
        payload.push(15);
        payload.push(0);
        write_raw_string(&mut payload, "release");
        payload.extend_from_slice(&123i64.to_le_bytes());
        payload.extend_from_slice(&0i32.to_le_bytes());
    }
    payload
}

fn write_raw_string(payload: &mut Vec<u8>, value: &str) {
    payload.extend_from_slice(&(value.len() as i32).to_le_bytes());
    payload.extend_from_slice(value.as_bytes());
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
