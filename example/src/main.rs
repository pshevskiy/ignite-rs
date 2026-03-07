#[cfg(not(feature = "ssl"))]
use ignite_rs::cache::Cache;
#[cfg(not(feature = "ssl"))]
use ignite_rs::ClientConfig;
#[cfg(not(feature = "ssl"))]
use ignite_rs_derive::IgniteObj;

#[cfg(not(feature = "ssl"))]
#[tokio::main]
async fn main() {
    // Create a client configuration
    let client_config = ClientConfig::new("localhost:10800");

    // Optionally define user, password, TCP configuration
    // client_config.username = Some("ignite".into());
    // client_config.password = Some("ignite".into());

    // Create an actual client. The protocol handshake is done here
    let ignite = ignite_rs::new_client(client_config).await.unwrap();

    // Get a list of present caches
    if let Ok(names) = ignite.get_cache_names().await {
        println!("ALL caches: {:?}", names)
    }

    // Create a typed cache named "test"
    let hello_cache: Cache<MyType, MyOtherType> = ignite
        .get_or_create_cache::<MyType, MyOtherType>("test")
        .await
        .unwrap();

    let key = MyType {
        bar: "AAAAA".into(),
        foo: 999,
    };
    let val = MyOtherType {
        list: vec![Some(FooBar {})],
        arr: vec![-23423423i64, -2342343242315i64],
    };

    // Put value
    hello_cache.put(&key, &val).await.unwrap();

    // Retrieve value
    println!("{:?}", hello_cache.get(&key).await.unwrap());
}

#[cfg(feature = "ssl")]
fn main() {
    eprintln!(
        "Default example is TCP-only. For TLS, run: cargo run --manifest-path crates/example/Cargo.toml --features ssl --bin tls_smoke"
    );
}

// Define your structs, that could be used as keys or values
#[cfg(not(feature = "ssl"))]
#[derive(IgniteObj, Clone, Debug)]
struct MyType {
    bar: String,
    foo: i32,
}

#[cfg(not(feature = "ssl"))]
#[derive(IgniteObj, Clone, Debug)]
struct MyOtherType {
    list: Vec<Option<FooBar>>,
    arr: Vec<i64>,
}

#[cfg(not(feature = "ssl"))]
#[derive(IgniteObj, Clone, Debug)]
struct FooBar {}
