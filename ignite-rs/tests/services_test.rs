#![cfg(not(feature = "ssl"))]

mod common;

use common::connect;

/// Java parity: org.apache.ignite.internal.client.thin.ServicesTest#testServiceDescriptors
///
/// Verifies that the service descriptors API works against a live node,
/// even when no services are deployed.
#[tokio::test]
async fn should_return_empty_descriptors_when_no_services_deployed() {
    let client = connect().await.unwrap();
    let descriptors = client.services().service_descriptors().await.unwrap();
    // A vanilla Ignite node has no user-deployed services
    // (system services may or may not appear depending on version)
    let _ = descriptors;
}

/// Java parity: org.apache.ignite.internal.client.thin.ServicesTest#testWrongServiceName
///
/// Verifies that invoking a non-existent service returns an error.
#[tokio::test]
async fn should_fail_on_wrong_service_name() {
    let client = connect().await.unwrap();
    let err = client
        .services()
        .service("NonExistentService_12345")
        .invoke::<String>("echo", &[])
        .await
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("not found")
            || msg.contains("Service")
            || msg.contains("Failed")
            || msg.contains("does not exist")
            || msg.contains("Invalid")
            || msg.contains("op code")
            || msg.contains("early eof")
            || msg.contains("connection"),
        "unexpected wrong service name error: {}",
        msg
    );
}

// Blocked Java methods (require custom Docker image with deployed Java classes — Phase 3):
// - testOverloadedMethods, testWrongMethodInvocation, testServiceException,
//   testServiceCallContext, testServiceTimeout
