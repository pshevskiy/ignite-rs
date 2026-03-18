#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, unique_name};
use ignite_rs::data_structures::CollectionConfiguration;

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testGetNonExistentSetReturnsNull
#[tokio::test]
async fn should_return_none_for_missing_set() {
    let client = connect().await.unwrap();
    let set = client
        .set::<i32>(&unique_name("missing_set"), None)
        .await
        .unwrap();

    assert!(set.is_none());
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testAddRemoveContains
#[tokio::test]
async fn should_add_remove_contains_and_size_items() {
    let client = connect().await.unwrap();
    let set = client
        .set::<i32>(
            &unique_name("ints"),
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .unwrap()
        .unwrap();

    assert!(set.add(&1).await.unwrap());
    assert!(set.contains(&1).await.unwrap());
    assert_eq!(set.size().await.unwrap(), 1);
    assert!(set.remove(&1).await.unwrap());
    assert!(!set.contains(&1).await.unwrap());
    set.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testCloseThenUseThrowsException
#[tokio::test]
async fn should_fail_after_close_and_report_removed() {
    let client = connect().await.unwrap();
    let name = unique_name("ints_closed");
    let set = client
        .set::<i32>(&name, Some(&CollectionConfiguration::new().with_backups(1)))
        .await
        .unwrap()
        .unwrap();
    let set2 = client.set::<i32>(&name, None).await.unwrap().unwrap();

    set.add(&1).await.unwrap();
    set.close().await.unwrap();

    assert!(set.removed().await.unwrap());
    assert!(set2.removed().await.unwrap());
    assert!(set
        .contains(&1)
        .await
        .unwrap_err()
        .to_string()
        .contains("does not exist"));
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testCloseAndCreateWithSameName
#[tokio::test]
async fn should_recreate_closed_set_with_same_name() {
    let client = connect().await.unwrap();
    let name = unique_name("ints_recreated");
    let old_set = client
        .set::<i32>(&name, Some(&CollectionConfiguration::new().with_backups(1)))
        .await
        .unwrap()
        .unwrap();

    old_set.add(&1).await.unwrap();
    old_set.close().await.unwrap();

    let new_set = client
        .set::<i32>(&name, Some(&CollectionConfiguration::new().with_backups(1)))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(new_set.size().await.unwrap(), 0);
    assert!(!new_set.removed().await.unwrap());
    new_set.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testAddAll
/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testContainsAll
/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testRemoveAll
/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testRetainAll
#[tokio::test]
async fn should_support_bulk_set_operations() {
    let client = connect().await.unwrap();
    let set = client
        .set::<i32>(
            &unique_name("ints_bulk"),
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .unwrap()
        .unwrap();

    assert!(set.add_all(&[1, 3]).await.unwrap());
    assert!(!set.add_all(&[1, 3]).await.unwrap());
    assert!(set.contains_all(&[1, 3]).await.unwrap());
    assert!(!set.contains_all(&[1, 2, 3]).await.unwrap());
    assert!(set.remove_all(&[1]).await.unwrap());
    assert!(!set.retain_all(&[3]).await.unwrap());
    assert_eq!(set.size().await.unwrap(), 1);
    set.close().await.unwrap();
}
