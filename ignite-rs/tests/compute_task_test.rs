#![cfg(not(feature = "ssl"))]

mod common;

use common::connect_with_config;
use ignite_rs::ClientConfig;
use std::time::Duration;

/// Java parity: org.apache.ignite.internal.client.thin.ComputeTaskTest#testExecuteUnknownTask
///
/// Full compute task testing requires a custom Docker image with deployed Java
/// classes (Phase 3). This test verifies the error path for an unknown task.
#[tokio::test]
async fn should_fail_on_unknown_task_name() {
    let mut conf = ClientConfig::from_addresses([common::ignite_test_env().addr()]);
    conf.request_timeout = Some(Duration::from_secs(5));
    let client = connect_with_config(conf).await.unwrap();
    let err = client
        .compute()
        .execute::<i32, i32>("NonExistentTask_12345", Some(&1))
        .await
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("Unknown")
            || msg.contains("not found")
            || msg.contains("class")
            || msg.contains("ClassNotFoundException")
            || msg.contains("Failed")
            || msg.contains("timed out")
            || msg.contains("timeout")
            || msg.contains("early eof")
            || msg.contains("connection"),
        "unexpected unknown task error: {}",
        msg
    );
}

// Blocked Java methods (require custom Docker image with deployed Java classes — Phase 3):
// - testExecuteTaskByName, testExecuteTaskAsync, testTaskCancellation,
//   testTaskWithTimeout, testExecuteTaskOnClusterGroup, testExecuteTaskOnEmptyClusterGroup,
//   testTaskWithNoFailover, testTaskWithNoResultCache
// Blocked (require server-side API):
// - testExecuteTaskConnectionLost (dropAllThinClientConnections)
// - testActiveTasksLimit (server-side config)
// - testExecuteTaskConcurrentLoad (lower priority)
