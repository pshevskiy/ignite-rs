#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    debug_context_registry_contains, debug_context_registry_key, debug_profile_descriptor,
    debug_prune_context_registry, ignite_context, FixtureScope, IgniteProfile,
};
use std::sync::Arc;

#[test]
fn should_reuse_process_scoped_context_within_test_process() {
    let ctx1 = ignite_context(
        IgniteProfile::CustomClientPort(39081),
        FixtureScope::Process,
    );
    let ctx2 = ignite_context(
        IgniteProfile::CustomClientPort(39081),
        FixtureScope::Process,
    );

    assert!(
        Arc::ptr_eq(&ctx1, &ctx2),
        "process-scoped context should be shared within one test binary"
    );
    assert_eq!(ctx1.scope(), FixtureScope::Process);
}

#[test]
fn should_reuse_cargo_session_context_within_test_process() {
    let ctx1 = ignite_context(
        IgniteProfile::CustomClientPort(39082),
        FixtureScope::CargoSession,
    );
    let ctx2 = ignite_context(
        IgniteProfile::CustomClientPort(39082),
        FixtureScope::CargoSession,
    );

    assert!(
        Arc::ptr_eq(&ctx1, &ctx2),
        "cargo-session context should be shared within one test binary"
    );
    assert_eq!(ctx1.scope(), FixtureScope::CargoSession);
}

#[test]
fn should_resolve_custom_client_port_as_external_process_context() {
    let port = 19080;
    let ctx = ignite_context(IgniteProfile::CustomClientPort(port), FixtureScope::Process);

    assert_eq!(ctx.scope(), FixtureScope::Process);
    assert_eq!(
        ctx.single_env()
            .expect("custom client port must resolve as single env")
            .addr(),
        format!("127.0.0.1:{port}")
    );
}

#[test]
fn should_describe_profile_defaults_through_descriptor_table() {
    let (single_key, single_kind, single_scope) =
        debug_profile_descriptor(IgniteProfile::DefaultSingleNode);
    assert_eq!(single_key, "single-node");
    assert_eq!(single_kind, "single");
    assert_eq!(single_scope, FixtureScope::CargoSession);

    let (cluster_key, cluster_kind, cluster_scope) =
        debug_profile_descriptor(IgniteProfile::ThreeNodeClusterChurn);
    assert_eq!(cluster_key, "cluster-3-churn");
    assert_eq!(cluster_kind, "cluster");
    assert_eq!(cluster_scope, FixtureScope::Process);
}

#[test]
fn should_key_context_registry_by_scope_and_fixture_identity() {
    let default_key =
        debug_context_registry_key(IgniteProfile::DefaultSingleNode, FixtureScope::CargoSession);
    let churn_key =
        debug_context_registry_key(IgniteProfile::SingleNodeChurn, FixtureScope::Process);
    let custom_10800 = debug_context_registry_key(
        IgniteProfile::CustomClientPort(10800),
        FixtureScope::Process,
    );
    let custom_10801 = debug_context_registry_key(
        IgniteProfile::CustomClientPort(10801),
        FixtureScope::Process,
    );

    assert_ne!(default_key, churn_key);
    assert_ne!(custom_10800, custom_10801);
    assert!(
        custom_10800.contains("127.0.0.1:10800"),
        "custom port identity should be embedded in the registry key: {}",
        custom_10800
    );
}

#[test]
fn should_prune_stale_weak_entries_from_context_registry() {
    let profile = IgniteProfile::CustomClientPort(29081);
    assert!(!debug_context_registry_contains(
        profile,
        FixtureScope::Process
    ));

    let ctx = ignite_context(profile, FixtureScope::Process);
    assert!(debug_context_registry_contains(
        profile,
        FixtureScope::Process
    ));

    drop(ctx);
    debug_prune_context_registry();

    assert!(
        !debug_context_registry_contains(profile, FixtureScope::Process),
        "dropped process-scoped context should be pruned from the registry"
    );
}
