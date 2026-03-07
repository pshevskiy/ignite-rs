#[cfg(feature = "ssl")]
use ignite_rs::new_client;
#[cfg(feature = "ssl")]
use ignite_rs::{client_config_from_ca_and_client_pem, client_config_from_ca_pem};

#[cfg(feature = "ssl")]
#[derive(serde::Deserialize)]
struct RootCfg { app: AppCfg }

#[cfg(feature = "ssl")]
#[derive(serde::Deserialize)]
struct AppCfg {
    #[serde(rename = "thin-client")]
    thin_client: ThinClientCfg,
}

#[cfg(feature = "ssl")]
#[derive(serde::Deserialize)]
struct ThinClientCfg {
    addresses: Vec<String>,
    #[serde(default)]
    tls: Option<TlsCfg>,
}

#[cfg(feature = "ssl")]
#[derive(serde::Deserialize, Clone)]
struct TlsCfg {
    #[serde(default)]
    ca_pem: Option<String>,
    #[serde(default)]
    server_name: Option<String>,
    #[serde(default)]
    client_cert_pem: Option<String>,
    #[serde(default)]
    client_key_pem: Option<String>,
}

#[cfg(feature = "ssl")]
#[tokio::main]
async fn main() {
    // application.yaml path (optional override via APP_YAML)
    let yaml_path = std::env::var("APP_YAML").unwrap_or_else(|_| "application.yaml".to_string());
    let s = std::fs::read_to_string(&yaml_path).expect("failed to read application.yaml");
    let cfg: RootCfg = serde_yaml::from_str(&s).expect("invalid application.yaml");

    let addr = cfg
        .app
        .thin_client
        .addresses
        .first()
        .expect("thin-client.addresses must contain at least one entry")
        .to_string();

    let tls = cfg.app.thin_client.tls.expect("thin-client.tls block required for tls_smoke");
    let ca = tls.ca_pem.expect("thin-client.tls.ca_pem is required");
    let sni = tls.server_name.expect("thin-client.tls.server_name is required");

    let client_conf = match (tls.client_cert_pem, tls.client_key_pem) {
        (Some(cert), Some(key)) => {
            client_config_from_ca_and_client_pem(&addr, &ca, &cert, &key, &sni).expect("invalid mTLS config")
        }
        _ => client_config_from_ca_pem(&addr, &ca, &sni).expect("invalid TLS config"),
    };

    let client = new_client(client_conf).await.expect("ignite connect failed");
    let names = client
        .get_cache_names()
        .await
        .expect("get_cache_names failed");
    println!("TLS smoke: {:?}", names);
}

#[cfg(not(feature = "ssl"))]
fn main() {
    eprintln!("tls_smoke requires building with --features ssl");
    std::process::exit(1);
}
