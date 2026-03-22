#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, unique_name};
use ignite_rs::cache::AtomicityMode;
use ignite_rs::data_structures::CollectionConfiguration;
use ignite_rs_derive::IgniteObj;

#[derive(Debug, Clone, PartialEq, IgniteObj)]
struct Person {
    id: i32,
    name: String,
}

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

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testUserObject
#[tokio::test]
async fn should_support_user_objects_in_set() {
    let client = connect().await.unwrap();
    let set = client
        .set::<Person>(
            &unique_name("person_set"),
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .unwrap()
        .unwrap();

    let alice = Person {
        id: 1,
        name: "Alice".to_string(),
    };
    let bob = Person {
        id: 2,
        name: "Bob".to_string(),
    };

    assert!(set.add(&alice).await.unwrap());
    assert!(set.add(&bob).await.unwrap());
    assert!(!set.add(&alice).await.unwrap());
    assert_eq!(set.size().await.unwrap(), 2);
    assert!(set.contains(&alice).await.unwrap());
    assert!(set.contains(&bob).await.unwrap());
    assert!(set.remove(&alice).await.unwrap());
    assert!(!set.contains(&alice).await.unwrap());
    assert_eq!(set.size().await.unwrap(), 1);
    set.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testConfigPropagation
#[tokio::test]
async fn should_propagate_collection_configuration() {
    let client = connect().await.unwrap();
    let grp = unique_name("grp_cfg");
    let cfg = CollectionConfiguration::new()
        .with_group_name(&grp)
        .with_backups(7)
        .with_atomicity_mode(AtomicityMode::Transactional)
        .with_colocated(true);
    let set = client
        .set::<i32>(&unique_name("cfg_set"), Some(&cfg))
        .await
        .unwrap()
        .unwrap();

    assert!(set.colocated());
    assert!(set.add(&42).await.unwrap());
    assert!(set.contains(&42).await.unwrap());
    assert_eq!(set.size().await.unwrap(), 1);
    set.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testSameNameInDifferentGroups
#[tokio::test]
async fn should_isolate_same_name_in_different_groups() {
    let client = connect().await.unwrap();
    let name = unique_name("shared_name");
    let set_a = client
        .set::<i32>(
            &name,
            Some(
                &CollectionConfiguration::new()
                    .with_backups(1)
                    .with_group_name("group_a"),
            ),
        )
        .await
        .unwrap()
        .unwrap();
    let set_b = client
        .set::<i32>(
            &name,
            Some(
                &CollectionConfiguration::new()
                    .with_backups(1)
                    .with_group_name("group_b"),
            ),
        )
        .await
        .unwrap()
        .unwrap();

    assert!(set_a.add(&1).await.unwrap());
    assert!(set_a.add(&2).await.unwrap());
    assert!(set_b.add(&10).await.unwrap());

    assert_eq!(set_a.size().await.unwrap(), 2);
    assert_eq!(set_b.size().await.unwrap(), 1);
    assert!(!set_b.contains(&1).await.unwrap());
    assert!(set_b.contains(&10).await.unwrap());

    set_a.close().await.unwrap();
    set_b.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testSameNameDifferentOptions
#[tokio::test]
async fn should_isolate_same_name_different_options() {
    let client = connect().await.unwrap();
    let name = unique_name("opts_set");
    let grp = unique_name("gp_opts");
    let set1 = client
        .set::<i32>(
            &name,
            Some(&CollectionConfiguration::new().with_group_name(&grp)),
        )
        .await
        .unwrap()
        .unwrap();

    let set2 = client
        .set::<i32>(
            &name,
            Some(
                &CollectionConfiguration::new()
                    .with_group_name(&grp)
                    .with_atomicity_mode(AtomicityMode::Transactional),
            ),
        )
        .await
        .unwrap()
        .unwrap();

    set1.add(&2).await.unwrap();
    set2.add(&3).await.unwrap();

    assert!(set1.contains(&2).await.unwrap());
    assert!(set2.contains(&3).await.unwrap());

    assert!(!set1.contains(&3).await.unwrap());
    assert!(!set2.contains(&1).await.unwrap());

    set1.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testIteratorEmpty
#[tokio::test]
async fn should_iterate_empty_set() {
    let client = connect().await.unwrap();
    let set = client
        .set::<i32>(
            &unique_name("empty_iter"),
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .unwrap()
        .unwrap();

    let items = set.iter().await.unwrap().fetch_all().await.unwrap();
    assert!(items.is_empty());
    set.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testIteratorClosesOnLastPage
#[tokio::test]
async fn should_close_iterator_on_last_page() {
    let client = connect().await.unwrap();
    let set = client
        .set::<i32>(
            &unique_name("iter_close_last"),
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .unwrap()
        .unwrap();

    set.add_all(&[1, 2, 3]).await.unwrap();

    let set = set.with_page_size(10).unwrap();
    let mut cursor = set.iter().await.unwrap();
    let page1 = cursor.next_page().await.unwrap();
    assert_eq!(page1.len(), 3);
    let page2 = cursor.next_page().await.unwrap();
    assert!(page2.is_empty());
    set.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testCloseBeforeEnd
#[tokio::test]
async fn should_close_iterator_before_end() {
    let client = connect().await.unwrap();
    let set = client
        .set::<i32>(
            &unique_name("iter_close_early"),
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .unwrap()
        .unwrap();

    set.add_all(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).await.unwrap();

    let set = set.with_page_size(3).unwrap();
    let mut cursor = set.iter().await.unwrap();
    let page = cursor.next_page().await.unwrap();
    assert!(!page.is_empty());
    cursor.close().await.unwrap();
    set.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testIteratorForeach
#[tokio::test]
async fn should_iterate_foreach() {
    let client = connect().await.unwrap();
    let set = client
        .set::<i32>(
            &unique_name("iter_foreach"),
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .unwrap()
        .unwrap();

    set.add_all(&[5, 3, 1, 4, 2]).await.unwrap();

    let mut items = set.iter().await.unwrap().fetch_all().await.unwrap();
    items.sort();
    assert_eq!(items, vec![1, 2, 3, 4, 5]);
    set.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testModifyWhileIterating
#[tokio::test]
async fn should_handle_modification_during_iteration() {
    let client = connect().await.unwrap();
    let set = client
        .set::<i32>(
            &unique_name("iter_modify"),
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .unwrap()
        .unwrap();

    set.add_all(&[1, 2, 3, 4, 5]).await.unwrap();

    let set = set.with_page_size(2).unwrap();
    let mut cursor = set.iter().await.unwrap();
    let page1 = cursor.next_page().await.unwrap();
    assert!(!page1.is_empty());

    // Modify the set while iterating.
    set.add_all(&[100, 200, 300]).await.unwrap();

    // Continue iteration — should not error.
    let mut all_items = page1;
    loop {
        let page = cursor.next_page().await.unwrap();
        if page.is_empty() {
            break;
        }
        all_items.extend(page);
    }

    // At minimum, the original items should be present.
    assert!(all_items.len() >= 5);
    set.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testToArrayEmpty
#[tokio::test]
async fn should_return_empty_to_array() {
    let client = connect().await.unwrap();
    let set = client
        .set::<i32>(
            &unique_name("to_array_empty"),
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .unwrap()
        .unwrap();

    let items = set.iter().await.unwrap().fetch_all().await.unwrap();
    assert!(items.is_empty());
    set.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testToArray
#[tokio::test]
async fn should_convert_to_array_with_various_page_sizes() {
    let client = connect().await.unwrap();
    let set = client
        .set::<i32>(
            &unique_name("to_array_pages"),
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .unwrap()
        .unwrap();

    set.add_all(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).await.unwrap();

    // page_size = 1
    let set = set.with_page_size(1).unwrap();
    let mut items = set.iter().await.unwrap().fetch_all().await.unwrap();
    items.sort();
    assert_eq!(items, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // page_size = 5
    let set = set.with_page_size(5).unwrap();
    let mut items = set.iter().await.unwrap().fetch_all().await.unwrap();
    items.sort();
    assert_eq!(items, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // page_size = 100
    let set = set.with_page_size(100).unwrap();
    let mut items = set.iter().await.unwrap().fetch_all().await.unwrap();
    items.sort();
    assert_eq!(items, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    set.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testAddAll
#[tokio::test]
async fn should_add_all() {
    let client = connect().await.unwrap();
    let set = client
        .set::<i32>(
            &unique_name("add_all"),
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .unwrap()
        .unwrap();

    assert!(set.add_all(&[1, 2, 3]).await.unwrap());
    assert_eq!(set.size().await.unwrap(), 3);
    assert!(!set.add_all(&[1, 2, 3]).await.unwrap());
    assert_eq!(set.size().await.unwrap(), 3);
    set.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testContainsAll
#[tokio::test]
async fn should_contains_all() {
    let client = connect().await.unwrap();
    let set = client
        .set::<i32>(
            &unique_name("contains_all"),
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .unwrap()
        .unwrap();

    set.add_all(&[1, 2, 3]).await.unwrap();
    assert!(set.contains_all(&[1, 2]).await.unwrap());
    assert!(set.contains_all(&[1, 2, 3]).await.unwrap());
    assert!(!set.contains_all(&[1, 2, 3, 4]).await.unwrap());
    set.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testRemoveAll
#[tokio::test]
async fn should_remove_all() {
    let client = connect().await.unwrap();
    let set = client
        .set::<i32>(
            &unique_name("remove_all"),
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .unwrap()
        .unwrap();

    set.add_all(&[1, 2, 3, 4, 5]).await.unwrap();
    assert!(set.remove_all(&[1, 2]).await.unwrap());
    assert_eq!(set.size().await.unwrap(), 3);
    assert!(!set.remove_all(&[1, 2]).await.unwrap());
    assert_eq!(set.size().await.unwrap(), 3);
    set.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.IgniteSetTest#testRetainAll
#[tokio::test]
async fn should_retain_all() {
    let client = connect().await.unwrap();
    let set = client
        .set::<i32>(
            &unique_name("retain_all"),
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .unwrap()
        .unwrap();

    set.add_all(&[1, 2, 3, 4, 5]).await.unwrap();
    assert!(set.retain_all(&[2, 4]).await.unwrap());
    assert_eq!(set.size().await.unwrap(), 2);
    assert!(set.contains_all(&[2, 4]).await.unwrap());
    assert!(!set.contains(&1).await.unwrap());
    assert!(!set.contains(&3).await.unwrap());
    assert!(!set.contains(&5).await.unwrap());
    assert!(!set.retain_all(&[2, 4]).await.unwrap());
    assert_eq!(set.size().await.unwrap(), 2);
    set.close().await.unwrap();
}
