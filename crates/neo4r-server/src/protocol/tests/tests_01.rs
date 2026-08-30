#![allow(unused_imports)]

use super::super::*;
use super::*;
use crate::execute_request;
use neo4r_db::{DatabaseConfig, Neo4rDatabaseHandle};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
pub(super) fn recover_tx_decisions_requires_backend_coordinator_in_protocol_executor() {
    let dir = temp_dir("server-recover-tx-protocol");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();

    assert_eq!(
        parse_request("LIST_TX_DECISIONS").unwrap(),
        BackendRequest::ListTransactionDecisions
    );
    assert_eq!(
        format_response(&BackendResponse::OkTransactionDecisions(
            "count=1 entries=tx=7 decision=commit participants=local@0#3".to_string()
        )),
        "OK\tTX_DECISIONS\tcount=1 entries=tx=7 decision=commit participants=local@0#3"
    );
    assert!(matches!(
        execute_request(&db, parse_request("LIST_TX_DECISIONS").unwrap()),
        BackendResponse::Err(message) if message.contains("requires a backend coordinator")
    ));
    assert_eq!(
        parse_request("RECOVER_TX_DECISIONS").unwrap(),
        BackendRequest::RecoverTransactionDecisions
    );
    assert_eq!(
        format_response(&BackendResponse::OkTransactionRecovery(3)),
        "OK\tTX_RECOVERY\t3"
    );
    assert_eq!(
        format_response(&BackendResponse::OkGossip(
            "2:query=127.0.0.1:17688:state=alive".to_string()
        )),
        "OK\tGOSSIP\t2:query=127.0.0.1:17688:state=alive"
    );
    assert!(matches!(
        execute_request(&db, parse_request("RECOVER_TX_DECISIONS").unwrap()),
        BackendResponse::Err(message) if message.contains("requires a backend coordinator")
    ));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn gossip_discovery_protocol_commands_parse_and_format() {
    assert_eq!(
        parse_request("GOSSIP_NODE\t4\t127.0.0.1:7690\t127.0.0.1:8690\t9\t30000").unwrap(),
        BackendRequest::GossipNode {
            server_id: 4,
            query_address: "127.0.0.1:7690".to_string(),
            replication_address: "127.0.0.1:8690".to_string(),
            incarnation: 9,
            ttl_ms: 30000,
        }
    );
    assert_eq!(
        parse_request("LIST_GOSSIP_NODES").unwrap(),
        BackendRequest::ListGossipNodes
    );
    assert_eq!(
        parse_request("GOSSIP_REFRESH_MEMBERSHIP").unwrap(),
        BackendRequest::GossipRefreshFromMembership
    );
    assert_eq!(
        format_response(&BackendResponse::OkGossip(
            "2:query=127.0.0.1:17688:state=alive".to_string()
        )),
        "OK\tGOSSIP\t2:query=127.0.0.1:17688:state=alive"
    );
}

#[test]
pub(super) fn install_routing_table_command_updates_cluster_status() {
    let dir = temp_dir("server-install-routing-table");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2).with_server_id(10)).unwrap();

    let response = execute_request(
        &db,
        parse_request("INSTALL_ROUTING_TABLE\t2\t0:10:11\t1:11:10").unwrap(),
    );

    assert_eq!(response, BackendResponse::OkUnit);
    let text = format_response(&execute_request(
        &db,
        parse_request("CLUSTER_STATUS").unwrap(),
    ));
    assert!(text.contains("routing_version=2"));
    assert!(text.contains("shard=0 primary=10 replicas=11 local=true local_primary=true"));
    assert!(text.contains("shard=1 primary=11 replicas=10 local=true local_primary=false"));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn install_routing_table_rejects_non_increasing_version() {
    let dir = temp_dir("server-install-routing-version");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1).with_server_id(1)).unwrap();

    let response = execute_request(
        &db,
        parse_request("INSTALL_ROUTING_TABLE\t1\t0:1:").unwrap(),
    );

    assert!(
        matches!(response, BackendResponse::Err(message) if message.contains("version must increase"))
    );
    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn index_catalog_commands_execute_against_database() {
    let dir = temp_dir("server-index-catalog");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 2, 2)).unwrap();

    assert_eq!(
        execute_request(
            &db,
            parse_request("CREATE_INDEX\tperson_name\tPerson\tname").unwrap(),
        ),
        BackendResponse::OkUnit
    );
    let version = db.index_catalog().unwrap().version;
    assert_eq!(
        execute_request(
            &db,
            parse_request("CREATE_INDEX\tperson_name\tPerson\tname\tIF_NOT_EXISTS").unwrap(),
        ),
        BackendResponse::OkUnit
    );
    assert_eq!(db.index_catalog().unwrap().version, version);
    assert!(matches!(
        parse_request("CREATE_INDEX\tperson_name\tPerson\tname\tUNKNOWN"),
        Err(message) if message.contains("IF_NOT_EXISTS")
    ));
    assert_eq!(
        execute_request(
            &db,
            parse_request("CREATE_VECTOR_INDEX\tdoc_embedding\tDocument\tembedding\t2\tcosine")
                .unwrap(),
        ),
        BackendResponse::OkUnit
    );
    assert_eq!(
        execute_request(
            &db,
            parse_request(
                "CREATE_VECTOR_INDEX\tdoc_embedding\tDocument\tembedding\t2\tcosine\tIF_NOT_EXISTS"
            )
            .unwrap(),
        ),
        BackendResponse::OkUnit
    );
    assert_eq!(
        parse_request("REBUILD_VECTOR_INDEX\tdoc_embedding").unwrap(),
        BackendRequest::RebuildVectorIndex {
            name: "doc_embedding".to_string()
        }
    );
    assert!(matches!(
        parse_request("REBUILD_VECTOR_INDEX\tdoc_embedding\textra"),
        Err(message) if message.contains("extra fields")
    ));
    assert_eq!(
        execute_request(
            &db,
            parse_request("REBUILD_VECTOR_INDEX\tdoc_embedding").unwrap(),
        ),
        BackendResponse::OkUnit
    );
    assert!(matches!(
        execute_request(
            &db,
            parse_request("REBUILD_VECTOR_INDEX\tperson_name").unwrap(),
        ),
        BackendResponse::Err(message) if message.contains("does not exist") || message.contains("not a vector index")
    ));
    assert_eq!(
        execute_request(
            &db,
            parse_request("CREATE_CONSTRAINT\tperson_email_unique\tPerson\temail").unwrap(),
        ),
        BackendResponse::OkUnit
    );
    assert_eq!(
        execute_request(
            &db,
            parse_request("CREATE_CONSTRAINT\tperson_email_unique\tPerson\temail\tIF_NOT_EXISTS")
                .unwrap(),
        ),
        BackendResponse::OkUnit
    );
    assert!(matches!(
        execute_request(
            &db,
            parse_request("CREATE_CONSTRAINT\tperson_email_unique\tPerson\tname\tIF_NOT_EXISTS")
                .unwrap(),
        ),
        BackendResponse::Err(message) if message.contains("different definition")
    ));
    assert_eq!(
        execute_request(&db, parse_request("REBUILD_VECTOR_INDEXES").unwrap()),
        BackendResponse::OkUnit
    );
    assert!(matches!(
        parse_request("VECTOR_INDEX_STATUS").unwrap(),
        BackendRequest::VectorIndexStatus { name: None }
    ));
    assert_eq!(
        parse_request("VECTOR_INDEX_STATUS\tdoc_embedding").unwrap(),
        BackendRequest::VectorIndexStatus {
            name: Some("doc_embedding".to_string())
        }
    );
    assert!(matches!(
        parse_request("VECTOR_INDEX_STATUS\tdoc_embedding\textra"),
        Err(message) if message.contains("extra fields")
    ));
    let BackendResponse::OkVectorIndexStatus(vector_status) =
        execute_request(&db, parse_request("VECTOR_INDEX_STATUS").unwrap())
    else {
        panic!("expected vector index status");
    };
    assert!(vector_status.contains("doc_embedding:Document:embedding"));
    assert!(vector_status.contains("dimensions=2"));
    assert!(vector_status.contains("metric=cosine"));
    let BackendResponse::OkVectorIndexStatus(vector_status) = execute_request(
        &db,
        parse_request("VECTOR_INDEX_STATUS\tdoc_embedding").unwrap(),
    ) else {
        panic!("expected vector index status");
    };
    assert_eq!(
        vector_status,
        "doc_embedding:Document:embedding:dimensions=2:metric=cosine:entries=0"
    );
    assert!(matches!(
        execute_request(&db, parse_request("VECTOR_INDEX_STATUS\tmissing").unwrap()),
        BackendResponse::Err(message) if message.contains("does not exist")
    ));
    let response = execute_request(&db, parse_request("LIST_INDEXES").unwrap());
    let BackendResponse::OkRows { count, debug_rows } = response else {
        panic!("expected index rows");
    };
    assert_eq!(count, 3);
    assert!(debug_rows.contains("person_name"));
    assert!(debug_rows.contains("doc_embedding"));
    assert!(debug_rows.contains("person_email_unique"));
    let version = db.index_catalog().unwrap().version;
    assert_eq!(
        execute_request(
            &db,
            parse_request("DROP_INDEX\tmissing_index\tIF_EXISTS").unwrap(),
        ),
        BackendResponse::OkUnit
    );
    assert_eq!(
        execute_request(
            &db,
            parse_request("DROP_CONSTRAINT\tmissing_constraint\tIF_EXISTS").unwrap(),
        ),
        BackendResponse::OkUnit
    );
    assert_eq!(db.index_catalog().unwrap().version, version);
    assert!(matches!(
        parse_request("DROP_INDEX\tmissing_index\tUNKNOWN"),
        Err(message) if message.contains("IF_EXISTS")
    ));
    assert!(matches!(
        execute_request(
            &db,
            parse_request("DROP_CONSTRAINT\tdoc_embedding\tIF_EXISTS").unwrap(),
        ),
        BackendResponse::Err(message) if message.contains("is not a constraint")
    ));
    assert_eq!(
        execute_request(
            &db,
            parse_request("DROP_CONSTRAINT\tperson_email_unique").unwrap(),
        ),
        BackendResponse::OkUnit
    );
    assert_eq!(
        execute_request(&db, parse_request("DROP_INDEX\tperson_name").unwrap()),
        BackendResponse::OkUnit
    );
    assert_eq!(
        execute_request(
            &db,
            parse_request("DROP_INDEX\tperson_name\tIF_EXISTS").unwrap(),
        ),
        BackendResponse::OkUnit
    );
    assert_eq!(
        execute_request(
            &db,
            parse_request("DROP_CONSTRAINT\tperson_email_unique\tIF_EXISTS").unwrap(),
        ),
        BackendResponse::OkUnit
    );
    assert!(matches!(
        execute_request(&db, parse_request("LIST_INDEXES").unwrap()),
        BackendResponse::OkRows { count: 1, .. }
    ));

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn node_label_commands_execute_against_database() {
    let dir = temp_dir("server-node-label-command");
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let response = execute_request(
        &db,
        parse_request("CREATE_NODE\tPerson\tname=s:Alice").unwrap(),
    );
    assert_eq!(response, BackendResponse::OkNode(0));

    assert_eq!(
        execute_request(&db, parse_request("ADD_NODE_LABEL\t0\tEmployee").unwrap()),
        BackendResponse::OkUnit
    );
    assert_eq!(
        db.query(r#"MATCH (n:Employee) WHERE n.name = "Alice" RETURN n.name"#)
            .unwrap()
            .len(),
        1
    );

    assert_eq!(
        execute_request(&db, parse_request("REMOVE_NODE_LABEL\t0\tPerson").unwrap()),
        BackendResponse::OkUnit
    );
    assert!(db
        .query(r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n.name"#)
        .unwrap()
        .is_empty());

    let _ = fs::remove_dir_all(dir);
}

#[test]
pub(super) fn writes_single_line_error_response() {
    let mut output = Vec::new();
    write_response(
        &mut output,
        &BackendResponse::Err("bad\trequest".to_string()),
    )
    .unwrap();

    assert_eq!(String::from_utf8(output).unwrap(), "ERR\tbad\\trequest\n");
}

#[test]
pub(super) fn formats_redirect_response() {
    let response = BackendResponse::Redirect(BackendRedirect {
        kind: RedirectKind::Moved,
        shard_id: 3,
        target_server_id: Some(2),
        address: Some("127.0.0.1:17688".to_string()),
        routing_version: 17,
        database: "tenant_a".to_string(),
        retryable: true,
    });

    assert_eq!(
        format_response(&response),
        "ERR\tMOVED\tshard=3\tleader=2\taddress=127.0.0.1:17688\trouting_version=17\townership_epoch=17\tdatabase=tenant_a\tretryable=true"
    );
}

pub(super) fn temp_dir(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    path.push(format!("neo4r-{prefix}-{}-{nanos}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    path
}
