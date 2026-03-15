## Test Fixtures

The integration suite can provision several live Apache Ignite environments automatically through shared Docker-API-backed fixtures. Tests should prefer the explicit context API in `tests/common/fixtures.rs`:

- `ignite_context(IgniteProfile::..., FixtureScope::...)` for JUnit-style shared setup
- `ignite_scope(IgniteProfile::...)` as the compatibility wrapper with the default scope for that profile
- `connect_profile(...)` when a test only needs a client and not the fixture handle

Recommended scope defaults:
- `FixtureScope::CargoSession` for expensive shared managed profiles such as the default single-node and 3-node cluster
- `FixtureScope::Process` for churn profiles and tests that intentionally stop or restart nodes

One-command matrix runner:
- Run the full deterministic test matrix from the workspace root with `cargo run --manifest-path ignite-rs/Cargo.toml -p xtask -- test-matrix`
- Run one bucket only with `cargo run --manifest-path ignite-rs/Cargo.toml -p xtask -- test-matrix --bucket <pure|single_node|cluster3|cluster3_churn|auth|ssl>`

Cleanup behavior:
- managed `CargoSession` fixtures use owner-PID leases and the last live owner removes shared containers, networks, and generated cluster config
- managed `Process` fixtures use process-local names and clean up when the last local context drops
- Unix test runs also register best-effort `atexit` plus `SIGINT` / `SIGTERM` cleanup hooks for managed fixtures

Profiles:
- plain single-node
- plain 3-node cluster
- auth-enabled single-node
- TLS single-node
- mutual-TLS single-node
- delayed-handshake proxy over a live single-node endpoint

External overrides:
- `IGNITE_ADDR`: plain single-node endpoint.
- `IGNITE_3NODE_ADDRS`: comma-separated 3-node endpoints.
- `IGNITE_AUTH_ADDR`, `IGNITE_AUTH_USERNAME`, `IGNITE_AUTH_PASSWORD`: auth-enabled endpoint and credentials.
- `IGNITE_TLS_ADDR`, `IGNITE_TLS_SERVER_NAME`, `IGNITE_TLS_CA_PEM`: TLS endpoint and trust material.
- `IGNITE_TLS_CLIENT_CERT_PEM`, `IGNITE_TLS_CLIENT_KEY_PEM`: mTLS client material.
- `IGNITE_DELAYED_HANDSHAKE_ADDR`: external endpoint used by the delayed-handshake proxy test.

Fixture overrides:
- `IGNITE_TEST_IMAGE`: override the container image name.
- `IGNITE_TEST_TAG`: override the container tag.
- `IGNITE_TEST_CONTAINER_NAME`: override the shared container base name used across integration test binaries.
- `IGNITE_TEST_START_RETRIES`: override the number of client connect retries while the node boots.
- `IGNITE_TEST_START_DELAY_MS`: override the sleep between startup retries in milliseconds.
- `IGNITE_TEST_LOCK_WAIT_MS`: override how long binaries wait for the shared fixture lock.
- `IGNITE_TEST_LOCK_STALE_MS`: override when a stale shared-fixture lock is force-cleared.

Runtime expectations:
- Managed fixtures talk to a Docker-compatible API endpoint, not the `docker` or `podman` CLI.
- If `DOCKER_HOST` is set, the fixture connects through that Docker API endpoint.
- If `DOCKER_HOST` is unset, the fixture prefers the default local socket for the current platform.
- `TESTCONTAINERS_HOST_OVERRIDE` can be used when the Docker API is remote but the published ports should be reached through a different host.
- For Podman, expose a Docker-compatible socket first; the fixture does not start `podman system service` for you.
- TLS and mTLS fixtures mount the bundled Ignite test keystores from the repository and use the client PEM/CA files from `ignite/modules/platforms/cpp/odbc-test/config/ssl`.
