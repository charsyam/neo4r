use super::*;

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
pub(super) fn replication_tls_hello_roundtrip_uses_tls_channel() {
    let dir = temp_dir("replication-tls-hello");
    fs::create_dir_all(&dir).unwrap();
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    fs::write(&cert_path, TEST_TLS_CERT).unwrap();
    fs::write(&key_path, TEST_TLS_KEY).unwrap();

    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(7)).unwrap();
    let backend = TcpBackend::new(db)
        .with_replication_tls_config(NativeTlsConfig {
            cert_path: cert_path.clone(),
            key_path,
            client_ca_path: None,
            require_client_auth: false,
        })
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || backend.serve_replication_listener_once(listener).unwrap());

    let identity = request_tls_replication_hello(
        &address.to_string(),
        Duration::from_secs(3),
        &ReplicationTlsConfig {
            server_name: "localhost".to_string(),
            ca_cert_path: cert_path,
            client_cert_path: None,
            client_key_path: None,
        },
    )
    .unwrap();

    assert_eq!(identity.server_id, 7);
    server.join().unwrap();
    let _ = fs::remove_dir_all(dir);
}
