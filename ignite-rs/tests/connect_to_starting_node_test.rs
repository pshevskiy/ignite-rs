#![cfg(not(feature = "ssl"))]

mod common;

use common::{ignite_scope, recv_event, unused_local_addr, IgniteProfile};
use ignite_rs::events::{ClientEvent, ConnectionEventKind, EventSubscriptions};
use ignite_rs::{new_client_with_events, ClientConfig};
use std::io;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

/// Java parity: org.apache.ignite.client.ConnectToStartingNodeTest#testClientConnectBeforeDiscoveryStart
#[tokio::test]
async fn should_retry_initial_connect_until_server_starts() {
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let target_addr = scope
        .single_env()
        .expect("expected single-node Ignite scope")
        .addr()
        .to_string();
    let delayed_proxy = DelayedStartProxy::spawn(target_addr, Duration::from_millis(250));

    let mut conf = ClientConfig::new(delayed_proxy.addr());
    conf.handshake_timeout = Some(Duration::from_millis(100));
    conf.reconnect_backoff = Some(Duration::from_millis(100));
    conf.retry_limit = 40;
    conf.event_subscriptions = EventSubscriptions {
        connection: true,
        request: false,
        lifecycle: false,
    };

    let started = Instant::now();
    let (client, mut events) = new_client_with_events(conf).await;

    let client = client.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(200),
        "client connected too early for delayed-start coverage: {:?}",
        elapsed
    );

    let mut saw_connect_failed = false;
    let mut saw_connected = false;
    let mut connection_kinds = Vec::new();

    for _ in 0..64 {
        match recv_event(&mut events).await {
            ClientEvent::Connection(event) => {
                connection_kinds.push(event.kind.clone());
                if event.kind == ConnectionEventKind::ConnectFailed {
                    saw_connect_failed = true;
                }
                if event.kind == ConnectionEventKind::Connected {
                    saw_connected = true;
                    break;
                }
            }
            _ => {}
        }
    }

    assert!(
        saw_connect_failed,
        "expected at least one failed startup connect attempt, saw {:?}",
        connection_kinds
    );
    assert!(
        saw_connected,
        "expected a successful connect event after retries, saw {:?}",
        connection_kinds
    );

    let _ = client.get_cache_names().await.unwrap();
}

struct DelayedStartProxy {
    addr: String,
    join: Option<thread::JoinHandle<()>>,
}

impl DelayedStartProxy {
    fn spawn(target_addr: String, delay: Duration) -> Self {
        let addr = unused_local_addr();
        let bind_addr = addr.clone();
        let join = thread::spawn(move || {
            thread::sleep(delay);
            let listener =
                TcpListener::bind(&bind_addr).expect("failed to bind delayed-start proxy");
            let (mut client_stream, _) = listener
                .accept()
                .expect("failed to accept delayed-start client connection");
            let mut server_stream =
                TcpStream::connect(&target_addr).expect("failed to connect delayed proxy upstream");
            let mut client_reader = client_stream
                .try_clone()
                .expect("failed to clone delayed proxy client stream");
            let mut server_writer = server_stream
                .try_clone()
                .expect("failed to clone delayed proxy upstream stream");

            let upstream = thread::spawn(move || {
                let _ = io::copy(&mut client_reader, &mut server_writer);
                let _ = server_writer.shutdown(Shutdown::Write);
            });

            let _ = io::copy(&mut server_stream, &mut client_stream);
            let _ = client_stream.shutdown(Shutdown::Write);
            let _ = upstream.join();
        });

        Self {
            addr,
            join: Some(join),
        }
    }

    fn addr(&self) -> &str {
        &self.addr
    }
}

impl Drop for DelayedStartProxy {
    fn drop(&mut self) {
        let _ = TcpStream::connect(self.addr());
        let _ = self.join.take();
    }
}
