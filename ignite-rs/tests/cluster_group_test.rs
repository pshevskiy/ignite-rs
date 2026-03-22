#![cfg(not(feature = "ssl"))]

mod common;

use common::connect_cluster3;

/// Java parity: ClusterGroupTest#testClusterNodeFields
#[tokio::test]
async fn should_list_all_cluster_nodes() {
    let client = connect_cluster3().await.unwrap();
    let cluster = client.cluster();

    let nodes = cluster.nodes().await.unwrap();
    assert!(
        nodes.len() >= 3,
        "expected at least 3 nodes, got {}",
        nodes.len()
    );

    for node in &nodes {
        assert!(!node.id.is_empty(), "node id should be non-empty");
        assert!(
            node.order > 0,
            "node order should be > 0, got {}",
            node.order
        );
        // is_local is true for the node handling the thin client connection;
        // the server-side isLocal() reflects the processing node, not the client.
    }
}

/// Java parity: ClusterGroupTest#testForServersForClients
#[tokio::test]
async fn should_filter_servers() {
    let client = connect_cluster3().await.unwrap();
    let cluster = client.cluster();

    let servers = cluster.for_servers().nodes().await.unwrap();
    assert_eq!(
        servers.len(),
        3,
        "expected 3 server nodes, got {}",
        servers.len()
    );
    for node in &servers {
        assert!(
            !node.is_client,
            "server filter should not return client nodes"
        );
    }

    let clients = cluster.for_clients().nodes().await.unwrap();
    assert!(
        clients.is_empty(),
        "expected no client nodes, got {}",
        clients.len()
    );
}

/// Java parity: ClusterGroupTest#testNodeById
#[tokio::test]
async fn should_select_node_by_id() {
    let client = connect_cluster3().await.unwrap();
    let cluster = client.cluster();

    let all = cluster.nodes().await.unwrap();
    assert!(!all.is_empty(), "cluster should have nodes");

    let target_id = all[0].id.clone();
    let filtered = cluster.for_node_id(&target_id).nodes().await.unwrap();
    assert_eq!(
        filtered.len(),
        1,
        "for_node_id should return exactly 1 node"
    );
    assert_eq!(filtered[0].id, target_id);
}

/// Java parity: ClusterGroupTest#testForNodeIds
#[tokio::test]
async fn should_select_nodes_by_ids() {
    let client = connect_cluster3().await.unwrap();
    let cluster = client.cluster();

    let all = cluster.nodes().await.unwrap();
    assert!(all.len() >= 2, "need at least 2 nodes for this test");

    let id1 = all[0].id.clone();
    let id2 = all[1].id.clone();
    let filtered = cluster
        .for_node_ids([id1.clone(), id2.clone()])
        .nodes()
        .await
        .unwrap();
    assert_eq!(
        filtered.len(),
        2,
        "for_node_ids with 2 ids should return 2 nodes"
    );

    let filtered_ids: Vec<&str> = filtered.iter().map(|n| n.id.as_str()).collect();
    assert!(filtered_ids.contains(&id1.as_str()));
    assert!(filtered_ids.contains(&id2.as_str()));
}

/// Java parity: ClusterGroupTest#testForOldest
#[tokio::test]
async fn should_select_oldest_node() {
    let client = connect_cluster3().await.unwrap();
    let cluster = client.cluster();

    let all = cluster.nodes().await.unwrap();
    let min_order = all.iter().map(|n| n.order).min().unwrap();

    let oldest = cluster.for_oldest().nodes().await.unwrap();
    assert_eq!(oldest.len(), 1, "for_oldest should return exactly 1 node");
    assert_eq!(
        oldest[0].order, min_order,
        "oldest node should have the minimum order"
    );
}

/// Java parity: ClusterGroupTest#testForYoungest
#[tokio::test]
async fn should_select_youngest_node() {
    let client = connect_cluster3().await.unwrap();
    let cluster = client.cluster();

    let all = cluster.nodes().await.unwrap();
    let max_order = all.iter().map(|n| n.order).max().unwrap();

    let youngest = cluster.for_youngest().nodes().await.unwrap();
    assert_eq!(
        youngest.len(),
        1,
        "for_youngest should return exactly 1 node"
    );
    assert_eq!(
        youngest[0].order, max_order,
        "youngest node should have the maximum order"
    );
}

/// Java parity: ClusterGroupTest#testForRandom
#[tokio::test]
async fn should_select_random_node() {
    let client = connect_cluster3().await.unwrap();
    let cluster = client.cluster();

    let all = cluster.nodes().await.unwrap();
    let all_ids: Vec<&str> = all.iter().map(|n| n.id.as_str()).collect();

    let random = cluster.for_random().nodes().await.unwrap();
    assert_eq!(random.len(), 1, "for_random should return exactly 1 node");
    assert!(
        all_ids.contains(&random[0].id.as_str()),
        "random node id should be among known node ids"
    );
}

/// Java parity: ClusterGroupTest#testForHost
#[tokio::test]
async fn should_filter_by_host() {
    let client = connect_cluster3().await.unwrap();
    let cluster = client.cluster();

    let all = cluster.nodes().await.unwrap();
    assert!(!all.is_empty(), "cluster should have nodes");

    let hostname = all[0]
        .host_names
        .first()
        .expect("node should have at least one hostname")
        .clone();

    let filtered = cluster.for_host(&hostname).nodes().await.unwrap();
    assert!(
        !filtered.is_empty(),
        "for_host should return at least 1 node for hostname '{}'",
        hostname
    );
    for node in &filtered {
        assert!(
            node.host_names.contains(&hostname),
            "filtered node should contain hostname '{}'",
            hostname
        );
    }
}

/// Java parity: ClusterGroupTest#testNodeIds
#[tokio::test]
async fn should_get_node_ids() {
    let client = connect_cluster3().await.unwrap();
    let cluster = client.cluster();

    let ids = cluster.group().node_ids().await.unwrap();
    assert_eq!(ids.len(), 3, "expected 3 node ids, got {}", ids.len());
    for id in &ids {
        assert!(!id.is_empty(), "node id should be non-empty");
    }
}

/// Java parity: ClusterGroupTest#testForFiltersCombinations
#[tokio::test]
async fn should_combine_filters() {
    let client = connect_cluster3().await.unwrap();
    let cluster = client.cluster();

    let servers = cluster.for_servers().nodes().await.unwrap();
    let min_order = servers.iter().map(|n| n.order).min().unwrap();

    let combined = cluster.for_servers().for_oldest().nodes().await.unwrap();
    assert_eq!(
        combined.len(),
        1,
        "for_servers().for_oldest() should return exactly 1 node"
    );
    assert!(
        !combined[0].is_client,
        "combined filter result should be a server"
    );
    assert_eq!(
        combined[0].order, min_order,
        "combined filter result should be the oldest server"
    );
}

/// Java parity: ClusterGroupTest#testClusterNodeCaching
#[tokio::test]
async fn should_cache_cluster_nodes() {
    let client = connect_cluster3().await.unwrap();
    let cluster = client.cluster();

    let first = cluster.nodes().await.unwrap();
    let second = cluster.nodes().await.unwrap();
    assert_eq!(
        first, second,
        "consecutive calls to nodes() should return the same result (cached)"
    );
}
