use super::*;
use neo4r_db::{DatabaseConfig, Neo4rDatabaseHandle};
use neo4r_server::{NativeTlsConfig, TcpBackend, TcpBackendConfig};
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

const TEST_TLS_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIIDSTCCAjGgAwIBAgIUP4P8WKU4GJXKk2+Cqwp7tI9vlqswDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDgyOTE4MTUzMFoXDTM2MDgy
NjE4MTUzMFowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAmnVsiYttdqA7iE1UGAK9ED8irhM0PVbrVlJO76J2JH0V
nkAnH7G4D7sFjoVlHOI/aQ3cDnteawjudiUOFJn8SKoMoLLomxtFV7x/UZE/gXYk
jjtMuvdQkX6tN7AIZ7+Z0eL2GGUGBbhPzWwiy4Z1Eu0eit8PAAY0WJDfhoxZPn66
VKOvl21o6qDWIObOvwG8M87gO4Zo19t5KRtaEkTUXKC9jr1ctRh66TmkapdnurBk
UlwmS0zEYlKRmAL97ZRkR3A5786XUR4+LgAAWek3oCElrALPND4/mg2aCTWb1wlQ
j2NTSjbkCHP1YG290GZVPRjZofAWOj/zBMyCsefz7wIDAQABo4GSMIGPMB0GA1Ud
DgQWBBQ07qyxfPuJE+j82VOrq5NzbPbCSTAfBgNVHSMEGDAWgBQ07qyxfPuJE+j8
2VOrq5NzbPbCSTAaBgNVHREEEzARgglsb2NhbGhvc3SHBH8AAAEwDAYDVR0TAQH/
BAIwADAOBgNVHQ8BAf8EBAMCBaAwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDQYJKoZI
hvcNAQELBQADggEBAHhccuZzYZ/ZBBXBAJTwR4PSDuNxb0q00GMkcYnq0j7U3tKC
bdKOQlHJh4cYpCVJ8SV8Wkuzgy2Lwjbhy00w3teTMOzgywgZAzp9euuL6gwr1o6Q
iXwhiMQHFMhF0IDbslV/ZpRYSkTZaJz5Hojzblb8plQ7/eBxAYSLHVYk/+HLwYiY
dTO2BEhl6y45kKw+66IUYsGKjr+YxqzBKGOQusbgrBDi2O+Kmw4FXKeRH408TaF/
ukjE+6w6ROIe1P/MYYAYFumqitUdv9IwSi77WkaLcog5qKxcdzicq3bMSYqMTBAN
QMWCPUdRHiY31xH3MiGMfsLuXmzhckEuBZVTTMQ=
-----END CERTIFICATE-----
"#;

const TEST_TLS_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCadWyJi212oDuI
TVQYAr0QPyKuEzQ9VutWUk7vonYkfRWeQCcfsbgPuwWOhWUc4j9pDdwOe15rCO52
JQ4UmfxIqgygsuibG0VXvH9RkT+BdiSOO0y691CRfq03sAhnv5nR4vYYZQYFuE/N
bCLLhnUS7R6K3w8ABjRYkN+GjFk+frpUo6+XbWjqoNYg5s6/AbwzzuA7hmjX23kp
G1oSRNRcoL2OvVy1GHrpOaRql2e6sGRSXCZLTMRiUpGYAv3tlGRHcDnvzpdRHj4u
AABZ6TegISWsAs80Pj+aDZoJNZvXCVCPY1NKNuQIc/Vgbb3QZlU9GNmh8BY6P/ME
zIKx5/PvAgMBAAECggEACyQShTGseBfi1pgcfpmrKtUhYPGOqfWVucWFeKbWibEI
bA02WGr+56RdDr4goHHRrBF6JwTcA+Sb7dakMNue648+CogZ258FPei61qNsGezv
gkWhthypNlovD7fseBTexfMhpOA1WOVF9rKCW5bIfzbhcv4+CTzpVETccpdRLcvL
RJWGKtgJz8looxSAPWf8mh4LZNTT1Q5jnILjKbmQHrUXkb+ZSu8WA+pzTeIzkrru
6o6BhP5Yx65MXboWyOjzbsYDvZXyMt5yUfj6aFWTNuXKQaN9CRUGmTqkku0fA1Ew
UJZ8/C8bY5iPARfi95wmld0vNajphbmwjTiTqUtCZQKBgQDY/7OWSP3x4ySkHOTq
yjanIWnEOhPFyleUgWsoV1y+xqBTCrzyrgMlaI0RwW4hjnpb0cwRpTr1SYIS/77D
R9vzGaZjhJD7b5hbJJsw2CwDSfsJ2U8YQaU1L2xjSzk7d0ImLDUZg/9pRZu7t7NS
peK2CLStqRtrWVntCDfAey1A3QKBgQC2ODKkivGDAdSFsUK5h7jWNoUQ7p3dtxvC
OmhEEQ0EI3eBiPrylzQ3ZbY3irFeuwv1AFCxS2HN0T5G0uG9nMhnqZB9v+MOSMjt
TFX9PDyHDjwvYfEV3j+HDwBSd1b/qn8nIOYq2xmBeIpOSialUDjM6rKbaVU2dSvN
7etor1l1OwKBgQDO1/5RdMZLud6FaN10SMiLyzfMSifq05NkBXElhRDs8NyGC6hM
Ez8Ae4ZstFrMNcnAmFzTPRLUGPuaLJmj/21TbtHB7u1cHuW1i1E63/QkNnLK0o+o
aXqXFdtVUrD9VBKD3IPJDJ97s1RdPR/72hAewHGpT5bJXuRvIvQxz7g1KQKBgF36
TeQe5MA0SW9KJKebH/Ea3TYGWtTmgyKBDRVN1fC0egYMp6BF62BGzNuIZEH/JgON
zhAiWKbVq9DLIjGwkoskIKk6NdhAIaCBJjgcwPrGlLO7R6OHpCv7yKa/ddcWD84W
YZ7osRbdHDeUdqn73c+Rm9wbTx9u/tCOTEMJbJHRAoGAHKwCxaTXvoxNWgQP92X2
JJcTPjRJg2Kp9KIPxsnq8eKqOanjvxdKHF6kNf3t1gagkdzK3OLmAooFGKSWr2U4
CKqDaaCFNRlneIyNU8PY4JbyDoGxPsb8X1kLgt4WBQM+BNNSh4uL40qWTRMV0mqg
aURnkfyfEDNt0cM30ooLoBs=
-----END PRIVATE KEY-----
"#;

#[test]
fn client_executes_query_and_decodes_rows() {
    let dir = temp_dir("client-sdk-query");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::with_config(
        db,
        TcpBackendConfig {
            default_page_size: 2,
            ..TcpBackendConfig::default()
        },
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        backend.handle_native_stream(stream).unwrap();
    });

    let mut client = Client::connect(address).unwrap();
    client.ping().unwrap();
    let mut params = QueryParams::new();
    params.insert("name".to_string(), Value::String("Alice".to_string()));
    let rows = client
        .execute_with_params("CREATE (n:Person {name: $name}) RETURN n.name", &params)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Alice".to_string())))
    );

    let rows = client.query("MATCH (n:Person) RETURN n.name").unwrap();
    assert_eq!(rows.len(), 1);
    assert!(client
        .profile("MATCH (n:Person) RETURN n", &QueryParams::new())
        .unwrap()
        .contains("rows=1"));
    assert!(client
        .query_plan("MATCH (n:Person) RETURN n", &QueryParams::new())
        .unwrap()
        .contains("access="));
    assert!(client
        .cluster_status()
        .unwrap()
        .contains("routing_version="));
    assert!(client
        .capabilities()
        .unwrap()
        .contains("ownership_epoch=true"));
    client.close().unwrap();
    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn client_connects_to_native_tls_server() {
    let dir = temp_dir("client-sdk-native-tls");
    fs::create_dir_all(&dir).unwrap();
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    fs::write(&cert_path, TEST_TLS_CERT).unwrap();
    fs::write(&key_path, TEST_TLS_KEY).unwrap();
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let backend = TcpBackend::with_config(
        db,
        TcpBackendConfig {
            default_page_size: 2,
            ..TcpBackendConfig::default()
        },
    )
    .with_native_tls_config(NativeTlsConfig {
        cert_path: cert_path.clone(),
        key_path: key_path.clone(),
        client_ca_path: None,
        require_client_auth: false,
    })
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        backend.handle_stream(stream).unwrap();
    });

    let mut client = Client::connect_tls(
        address,
        NativeTlsClientConfig {
            server_name: "localhost".to_string(),
            ca_cert_path: cert_path,
            client_cert_path: None,
            client_key_path: None,
        },
    )
    .unwrap();
    client.ping().unwrap();
    let mut params = QueryParams::new();
    params.insert("name".to_string(), Value::String("Tls Alice".to_string()));
    let rows = client
        .query_with_params("CREATE (n:Person {name: $name}) RETURN n.name", &params)
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("n.name"),
        Some(&QueryValue::Scalar(Value::String("Tls Alice".to_string())))
    );
    client.close().unwrap();
    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}

#[test]
fn client_parses_redirect_response() {
    let redirect = parse_redirect_response(
            "ERR\tMOVED\tshard=3\tleader=2\taddress=127.0.0.1:17688\trouting_version=17\tdatabase=tenant_a\tretryable=true",
        )
        .unwrap();
    assert_eq!(redirect.kind, "MOVED");
    assert_eq!(redirect.shard_id, 3);
    assert_eq!(redirect.leader, Some(2));
    assert_eq!(redirect.address.as_deref(), Some("127.0.0.1:17688"));
    assert_eq!(redirect.routing_version, 17);
    assert_eq!(redirect.ownership_epoch, 17);
    assert_eq!(redirect.database, "tenant_a");
    assert!(redirect.retryable);
}

#[test]
fn client_parses_typed_stale_epoch_response() {
    let redirect = parse_redirect_response(
            "ERR\tSTALE_EPOCH\ttx_epoch=1\tcurrent_epoch=2\trouting_version=2\townership_epoch=2\tretryable=true",
        )
        .unwrap();
    assert_eq!(redirect.kind, "STALE_EPOCH");
    assert_eq!(redirect.routing_version, 2);
    assert_eq!(redirect.ownership_epoch, 2);
    assert!(redirect.retryable);
}

#[test]
fn client_follows_redirect_once() {
    let target_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let target_addr = target_listener.local_addr().unwrap();
    let target_server = thread::spawn(move || {
        let (mut stream, _) = target_listener.accept().unwrap();
        let frame = read_frame(&mut stream).unwrap().unwrap();
        assert_eq!(frame.message_type, NativeMessageType::Ping);
        write_frame(
            &mut stream,
            &NativeFrame::new(
                NativeMessageType::Response,
                frame.request_id,
                b"OK\tPONG".to_vec(),
            ),
        )
        .unwrap();
    });

    let redirect_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let redirect_addr = redirect_listener.local_addr().unwrap();
    let redirect_server = thread::spawn(move || {
        let (mut stream, _) = redirect_listener.accept().unwrap();
        let frame = read_frame(&mut stream).unwrap().unwrap();
        write_frame(
                &mut stream,
                &NativeFrame::new(
                    NativeMessageType::Error,
                    frame.request_id,
                    format!(
                        "ERR\tMOVED\tshard=0\tleader=2\taddress={target_addr}\trouting_version=1\tdatabase=default\tretryable=true"
                    )
                    .into_bytes(),
                ),
            )
            .unwrap();
    });

    let mut client = Client::connect(redirect_addr).unwrap();
    client.ping().unwrap();
    assert_eq!(client.topology_cache().routing_version, 1);
    assert_eq!(client.topology_cache().ownership_epoch, 1);

    redirect_server.join().unwrap();
    target_server.join().unwrap();
}

#[test]
fn client_rejects_redirect_loop() {
    let loop_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let loop_addr = loop_listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = loop_listener.accept().unwrap();
            let frame = read_frame(&mut stream).unwrap().unwrap();
            write_frame(
                    &mut stream,
                    &NativeFrame::new(
                        NativeMessageType::Error,
                        frame.request_id,
                        format!(
                            "ERR\tMOVED\tshard=0\tleader=1\taddress={loop_addr}\trouting_version=2\townership_epoch=2\tdatabase=default\tretryable=true"
                        )
                        .into_bytes(),
                    ),
                )
                .unwrap();
        }
    });

    let mut client = Client::connect(loop_addr).unwrap();
    let err = client.ping().unwrap_err();
    assert!(format!("{err}").contains("redirect loop detected"));
    server.join().unwrap();
}

#[test]
fn client_registry_address_prefers_remote_query_peer() {
    let address = first_registry_address(
        Some(1),
        Some("1:127.0.0.1:17687|2:127.0.0.1:17688"),
        Some("1:active:127.0.0.1:17687|2:active:127.0.0.1:17688"),
    );
    assert_eq!(address.as_deref(), Some("127.0.0.1:17688"));
}

#[test]
fn client_registry_address_falls_back_to_active_remote_node() {
    let address = first_registry_address(
        Some(1),
        Some("none"),
        Some("1:active:127.0.0.1:17687|2:active:127.0.0.1:17688|3:down:127.0.0.1:17689"),
    );
    assert_eq!(address.as_deref(), Some("127.0.0.1:17688"));
}

fn temp_dir(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("neo4r-{prefix}-{}-{nanos}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    path
}
