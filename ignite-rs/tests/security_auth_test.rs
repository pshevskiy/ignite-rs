#![cfg(feature = "ssl")]

mod common;

use common::{connect_auth, ignite_auth_env, unique_name};
use ignite_rs::error::ErrorKind;
use ignite_rs::query::{SqlFieldsQuery, SqlValue};
use ignite_rs::{new_client, ClientConfig};
use std::sync::OnceLock;

static SECURITY_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

/// Java parity: org.apache.ignite.client.SecurityTest#testInvalidUserAuthentication
#[tokio::test]
async fn should_reject_invalid_user_authentication() {
    let _guard = security_test_lock().lock().await;
    ensure_security_test_user().await;

    let err = match connect_auth_user("JOE", "password").await {
        Ok(_) => panic!("expected auth failure"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::Authentication);
}

/// Java parity: org.apache.ignite.client.SecurityTest#testInvalidUserAuthenticationAsync
#[tokio::test]
async fn should_reject_invalid_user_authentication_on_awaited_async_path() {
    let _guard = security_test_lock().lock().await;
    ensure_security_test_user().await;

    let err = tokio::spawn(async move { connect_auth_user("JOE", "password").await })
        .await
        .expect("auth future panicked");
    let err = match err {
        Ok(_) => panic!("expected auth failure"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::Authentication);
}

/// Java parity: org.apache.ignite.client.SecurityTest#testValidUserAuthentication
#[tokio::test]
async fn should_accept_valid_user_authentication() {
    let _guard = security_test_lock().lock().await;
    ensure_security_test_user().await;

    let client = connect_auth_user("joe", "password")
        .await
        .expect("expected auth success");
    client
        .get_or_create_cache::<i32, i32>(&unique_name("testAuthentication"))
        .await
        .expect("expected cache creation to succeed");
}

/// Java parity: org.apache.ignite.client.SecurityTest#testUserCannotCreateUser
#[tokio::test]
async fn should_reject_create_user_for_non_admin_user() {
    let _guard = security_test_lock().lock().await;
    ensure_security_test_user().await;

    let client = connect_auth_user("joe", "password")
        .await
        .expect("expected auth success");
    let user_to_create = unique_name("joe2").to_lowercase();
    let err = match client
        .sql_fields::<Vec<SqlValue>>(SqlFieldsQuery::new(&format!(
            "CREATE USER \"{}\" WITH PASSWORD 'password'",
            user_to_create
        )))
        .await
    {
        Ok(_) => panic!("expected create user to be rejected"),
        Err(err) => err,
    };

    assert_eq!(err.kind(), ErrorKind::Server);
    assert!(
        err.to_string()
            .contains("User management operations are not allowed"),
        "unexpected permission error: {}",
        err
    );
}

async fn ensure_security_test_user() {
    let admin = connect_auth()
        .await
        .expect("expected auth fixture admin client to connect");

    let create_user =
        SqlFieldsQuery::<Vec<SqlValue>>::new("CREATE USER \"joe\" WITH PASSWORD 'password'");

    match admin.sql_fields(create_user).await {
        Ok(cursor) => {
            let _ = cursor
                .fetch_all()
                .await
                .expect("expected CREATE USER bootstrap cursor to complete");
        }
        Err(err) => {
            let msg = err.to_string();
            assert!(
                msg.contains("User already exists"),
                "failed to bootstrap live auth user joe/password: {}",
                err
            );
        }
    }
}

async fn connect_auth_user(
    username: &str,
    password: &str,
) -> ignite_rs::error::IgniteResult<ignite_rs::Client> {
    let env = ignite_auth_env();
    env.wait_for_ready().await?;

    let mut conf: ClientConfig = env.client_config()?;
    conf.username = Some(username.to_string());
    conf.password = Some(password.to_string());

    new_client(conf).await
}

fn security_test_lock() -> &'static tokio::sync::Mutex<()> {
    SECURITY_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}
