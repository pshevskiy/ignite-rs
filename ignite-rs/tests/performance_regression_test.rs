#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, unique_name};
use ignite_rs::error::IgniteResult;
use ignite_rs::protocol::TypeCode;
use ignite_rs::{ReadableType, WritableType};
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct CountingKey {
    value: i32,
    writes: Arc<AtomicUsize>,
}

impl CountingKey {
    fn new(value: i32) -> Self {
        Self {
            value,
            writes: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn writes(&self) -> usize {
        self.writes.load(Ordering::Relaxed)
    }
}

impl WritableType for CountingKey {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.value.write(writer)
    }

    fn size(&self) -> usize {
        self.value.size()
    }
}

impl ReadableType for CountingKey {
    fn read_unwrapped(type_code: TypeCode, reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        Ok(i32::read_unwrapped(type_code, reader)?.map(Self::new))
    }
}

#[tokio::test]
async fn should_serialize_single_key_hot_path_operations_once() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("prepared_key_hot_path");
    let cache = client
        .get_or_create_cache::<CountingKey, i32>(&cache_name)
        .await
        .unwrap();

    let put_key = CountingKey::new(1);
    cache.put(&put_key, &1).await.unwrap();
    assert_eq!(put_key.writes(), 1);

    let get_key = CountingKey::new(2);
    assert_eq!(cache.get(&get_key).await.unwrap(), None);
    assert_eq!(get_key.writes(), 1);

    let contains_key = CountingKey::new(3);
    assert!(!cache.contains_key(&contains_key).await.unwrap());
    assert_eq!(contains_key.writes(), 1);

    let clear_key = CountingKey::new(4);
    cache.clear_key(&clear_key).await.unwrap();
    assert_eq!(clear_key.writes(), 1);

    client.destroy_cache(&cache_name).await.unwrap();
}
