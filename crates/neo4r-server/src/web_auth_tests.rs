use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("neo4r-web-auth-{name}-{nanos}"))
}

#[test]
fn web_user_token_codec_preserves_metadata_and_database_roles() {
    let mut database_roles = BTreeMap::new();
    database_roles.insert("tenant_a".to_string(), WebRole::Reader);
    database_roles.insert("*".to_string(), WebRole::Writer);
    let record = WebUserToken {
        name: "alice".to_string(),
        token_id: "main".to_string(),
        role: WebRole::Admin,
        token: "secret".to_string(),
        expired_at: 123,
        revoked: false,
        database_roles,
        denied_databases: BTreeSet::new(),
        created_at: 456,
        last_used_at: 789,
    };

    let encoded = encode_web_user_token(&record);
    let decoded = decode_web_user_token(encoded.as_bytes()).unwrap();

    assert_eq!(decoded, record);
}

#[test]
fn legacy_web_user_token_records_decode_with_zero_metadata() {
    let decoded =
        decode_web_user_token(b"alice\tmain\twriter\tsecret\t0\tfalse\ttenant_a=reader").unwrap();

    assert_eq!(decoded.name, "alice");
    assert_eq!(decoded.role_for_database("tenant_a"), Some(WebRole::Reader));
    assert_eq!(decoded.created_at, 0);
    assert_eq!(decoded.last_used_at, 0);
}

#[test]
fn web_roles_enforce_reader_writer_admin_boundaries() {
    assert!(WebRole::Reader.allows(WebRole::Reader));
    assert!(!WebRole::Reader.allows(WebRole::Writer));
    assert!(!WebRole::Reader.allows(WebRole::Admin));
    assert!(WebRole::Writer.allows(WebRole::Reader));
    assert!(WebRole::Writer.allows(WebRole::Writer));
    assert!(!WebRole::Writer.allows(WebRole::Admin));
    assert!(WebRole::Admin.allows(WebRole::Reader));
    assert!(WebRole::Admin.allows(WebRole::Writer));
    assert!(WebRole::Admin.allows(WebRole::Admin));
}

#[test]
fn web_user_token_store_updates_last_used_only_for_authorized_database() {
    let dir = temp_dir("touch-last-used");
    let store = WebUserTokenStore::open(dir.clone()).unwrap();
    let mut database_roles = BTreeMap::new();
    database_roles.insert("tenant_a".to_string(), WebRole::Reader);
    store
        .put(WebUserToken {
            name: "alice".to_string(),
            token_id: "main".to_string(),
            role: WebRole::Writer,
            token: "secret".to_string(),
            expired_at: 0,
            revoked: false,
            database_roles,
            denied_databases: BTreeSet::new(),
            created_at: 10,
            last_used_at: 0,
        })
        .unwrap();

    assert_eq!(store.find_role_by_token("secret", "tenant_b", 20), None);
    assert_eq!(store.list().unwrap()[0].last_used_at, 0);
    assert_eq!(
        store.find_role_by_token("secret", "tenant_a", 21),
        Some(WebRole::Reader)
    );
    assert_eq!(store.list().unwrap()[0].last_used_at, 21);

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn web_user_token_store_denies_database_before_grant_or_wildcard() {
    let dir = temp_dir("deny-precedence");
    let store = WebUserTokenStore::open(dir.clone()).unwrap();
    let mut database_roles = BTreeMap::new();
    database_roles.insert("*".to_string(), WebRole::Writer);
    store
        .put(WebUserToken {
            name: "alice".to_string(),
            token_id: "main".to_string(),
            role: WebRole::Reader,
            token: "secret".to_string(),
            expired_at: 0,
            revoked: false,
            database_roles,
            denied_databases: BTreeSet::new(),
            created_at: 10,
            last_used_at: 0,
        })
        .unwrap();

    store.deny_database("alice", "main", "tenant_a").unwrap();
    assert_eq!(store.find_role_by_token("secret", "tenant_a", 20), None);
    assert_eq!(
        store.find_role_by_token("secret", "tenant_b", 20),
        Some(WebRole::Writer)
    );
    store.allow_database("alice", "main", "tenant_a").unwrap();
    assert_eq!(
        store.find_role_by_token("secret", "tenant_a", 21),
        Some(WebRole::Writer)
    );

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn web_user_token_store_keeps_digest_instead_of_plaintext_token() {
    let dir = temp_dir("digest-only-token");
    let store = WebUserTokenStore::open(dir.clone()).unwrap();
    store
        .put(WebUserToken {
            name: "alice".to_string(),
            token_id: "main".to_string(),
            role: WebRole::Writer,
            token: "secret-token".to_string(),
            expired_at: 0,
            revoked: false,
            database_roles: BTreeMap::new(),
            denied_databases: BTreeSet::new(),
            created_at: 10,
            last_used_at: 0,
        })
        .unwrap();

    assert_eq!(
        store.find_role_by_token("secret-token", "default", 20),
        Some(WebRole::Writer)
    );
    let stored = store.list().unwrap().remove(0);
    assert_ne!(stored.token, "secret-token");
    assert_eq!(stored.token, web_token_digest("secret-token"));

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn token_digest_is_stable_and_constant_time_compare_matches_plain_equality() {
    assert_eq!(web_token_digest("secret"), web_token_digest("secret"));
    assert_ne!(web_token_digest("secret"), web_token_digest("other"));
    assert!(constant_time_token_eq("secret", "secret"));
    assert!(!constant_time_token_eq("secret", "secreu"));
    assert!(!constant_time_token_eq("secret", "secret-longer"));
}

#[test]
fn web_audit_store_prunes_events_older_than_cutoff() {
    let dir = temp_dir("audit-prune");
    let store = WebAuditStore::open(dir.clone()).unwrap();
    {
        let mut kv = store.kv.lock().unwrap();
        let old = WebAuditEvent {
            unix_ms: 10,
            action: "old".to_string(),
            target: "target".to_string(),
            detail: "detail".to_string(),
        };
        let new = WebAuditEvent {
            unix_ms: 20,
            action: "new".to_string(),
            target: "target".to_string(),
            detail: "detail".to_string(),
        };
        kv.put(
            &web_audit_key(old.unix_ms, &old.action, &old.target),
            encode_web_audit_event(&old).as_bytes(),
        )
        .unwrap();
        kv.put(
            &web_audit_key(new.unix_ms, &new.action, &new.target),
            encode_web_audit_event(&new).as_bytes(),
        )
        .unwrap();
    }

    assert_eq!(store.prune_older_than(15).unwrap(), 1);
    let events = store.list().unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, "new");
    let _ = std::fs::remove_dir_all(dir);
}
