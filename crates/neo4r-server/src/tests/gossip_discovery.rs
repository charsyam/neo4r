#![allow(unused_imports)]
use super::*;

#[test]
pub(super) fn gossip_fanout_sends_native_command_to_seed_peer() {
    let local_dir = temp_dir("neo4r-gossip-fanout-local");
    let seed_dir = temp_dir("neo4r-gossip-fanout-seed");
    let token = "super-secret-gossip-token".to_string();

    let local_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&local_dir, 1, 1).with_server_id(1)).unwrap();
    let seed_db =
        Neo4rDatabaseHandle::open(DatabaseConfig::new(&seed_dir, 1, 1).with_server_id(2)).unwrap();
    let seed_backend = TcpBackend::new(seed_db.clone()).with_gossip_auth_token(Some(token.clone()));
    let serving_backend = seed_backend.clone();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let seed_address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || serving_backend.serve_listener_once(listener).unwrap());

    let local_backend = TcpBackend::new(local_db.clone()).with_gossip_auth_token(Some(token));
    let result = local_backend.fanout_gossip_once(
        1,
        "127.0.0.1:17687",
        "127.0.0.1:18687",
        30_000,
        &[seed_address],
    );

    server.join().unwrap();
    assert_eq!(result.attempted, 1);
    assert_eq!(result.succeeded, 1);
    assert_eq!(result.failed, 0, "{:?}", result.errors);

    let BackendResponse::OkGossip(nodes) =
        seed_backend.execute_backend_request(parse_request("LIST_GOSSIP_NODES").unwrap())
    else {
        panic!("expected gossip node list");
    };
    assert!(nodes.contains("1:query=127.0.0.1:17687"));
    assert!(nodes.contains("replication=127.0.0.1:18687"));
    assert_eq!(
        seed_backend.list_query_peers().unwrap(),
        vec![(1, "127.0.0.1:17687".to_string())]
    );

    drop(local_db);
    drop(seed_db);
    let _ = fs::remove_dir_all(local_dir);
    let _ = fs::remove_dir_all(seed_dir);
}
