use neo4r_storage::{KeyValueStore, RocksKvStore};
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) const WEB_AUTH_ROCKS_DIR: &str = "system/web-auth-rocksdb";
pub(super) const WEB_AUDIT_ROCKS_DIR: &str = "system/web-audit-rocksdb";
pub(super) const WEB_SESSION_ROCKS_DIR: &str = "system/web-session-rocksdb";
const WEB_AUTH_USER_PREFIX: &[u8] = b"web/user/";
const WEB_AUTH_TOKEN_PREFIX: &[u8] = b"web/token/";
const WEB_AUDIT_PREFIX: &[u8] = b"web/audit/";
const WEB_SESSION_PREFIX: &[u8] = b"web/session/";

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub(super) enum WebRole {
    Reader,
    Writer,
    Admin,
}

impl WebRole {
    pub(super) fn allows(self, required: WebRole) -> bool {
        self >= required
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Reader => "reader",
            Self::Writer => "writer",
            Self::Admin => "admin",
        }
    }
}

pub(super) fn web_role_from_token(token: &str) -> WebRole {
    match token.split_once(':').map(|(role, _)| role) {
        Some(role) if role.eq_ignore_ascii_case("reader") => WebRole::Reader,
        Some(role) if role.eq_ignore_ascii_case("writer") => WebRole::Writer,
        Some(role) if role.eq_ignore_ascii_case("admin") => WebRole::Admin,
        _ => WebRole::Admin,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct WebUserToken {
    pub(super) name: String,
    pub(super) token_id: String,
    pub(super) role: WebRole,
    pub(super) token: String,
    pub(super) expired_at: u128,
    pub(super) revoked: bool,
    pub(super) database_roles: BTreeMap<String, WebRole>,
    pub(super) created_at: u128,
    pub(super) last_used_at: u128,
}

#[derive(Clone)]
pub(super) struct WebUserTokenStore {
    kv: Arc<Mutex<RocksKvStore>>,
}

#[derive(Clone)]
pub(super) struct WebAuditStore {
    kv: Arc<Mutex<RocksKvStore>>,
}

#[derive(Clone)]
pub(super) struct WebSessionStore {
    kv: Arc<Mutex<RocksKvStore>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct WebSession {
    pub(super) session_id: String,
    pub(super) csrf_token: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct WebAuditEvent {
    pub(super) unix_ms: u128,
    pub(super) action: String,
    pub(super) target: String,
    pub(super) detail: String,
}

impl WebAuditStore {
    pub(super) fn open(path: PathBuf) -> Result<Self, String> {
        ensure_store_parent(&path)?;
        RocksKvStore::open(path)
            .map(|kv| Self {
                kv: Arc::new(Mutex::new(kv)),
            })
            .map_err(|err| err.to_string())
    }

    pub(super) fn append(&self, action: &str, target: &str, detail: &str) -> Result<(), String> {
        let event = WebAuditEvent {
            unix_ms: unix_millis_now(),
            action: action.to_string(),
            target: target.to_string(),
            detail: detail.to_string(),
        };
        let mut kv = self
            .kv
            .lock()
            .map_err(|_| "web audit store lock poisoned".to_string())?;
        kv.put(
            &web_audit_key(event.unix_ms, action, target),
            encode_web_audit_event(&event).as_bytes(),
        )
        .map_err(|err| err.to_string())
    }

    pub(super) fn list(&self) -> Result<Vec<WebAuditEvent>, String> {
        let kv = self
            .kv
            .lock()
            .map_err(|_| "web audit store lock poisoned".to_string())?;
        kv.scan_prefix(WEB_AUDIT_PREFIX)
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(_, value)| decode_web_audit_event(&value))
            .collect()
    }

    pub(super) fn prune_older_than(&self, cutoff_unix_ms: u128) -> Result<usize, String> {
        let mut kv = self
            .kv
            .lock()
            .map_err(|_| "web audit store lock poisoned".to_string())?;
        let keys = kv
            .scan_prefix(WEB_AUDIT_PREFIX)
            .map_err(|err| err.to_string())?
            .into_iter()
            .filter_map(|(key, value)| {
                decode_web_audit_event(&value)
                    .ok()
                    .filter(|event| event.unix_ms < cutoff_unix_ms)
                    .map(|_| key)
            })
            .collect::<Vec<_>>();
        let removed = keys.len();
        for key in keys {
            kv.delete(&key).map_err(|err| err.to_string())?;
        }
        Ok(removed)
    }
}

impl WebUserTokenStore {
    pub(super) fn open(path: PathBuf) -> Result<Self, String> {
        ensure_store_parent(&path)?;
        RocksKvStore::open(path)
            .map(|kv| Self {
                kv: Arc::new(Mutex::new(kv)),
            })
            .map_err(|err| err.to_string())
    }

    pub(super) fn list(&self) -> Result<Vec<WebUserToken>, String> {
        let kv = self
            .kv
            .lock()
            .map_err(|_| "web token store lock poisoned".to_string())?;
        let mut users = kv
            .scan_prefix(WEB_AUTH_USER_PREFIX)
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|(_, value)| decode_web_user_token(&value))
            .collect::<Result<Vec<_>, _>>()?;
        users.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.token_id.cmp(&right.token_id))
        });
        Ok(users)
    }

    pub(super) fn find_role_by_token(
        &self,
        token: &str,
        database: &str,
        now_unix_seconds: u128,
    ) -> Option<WebRole> {
        let record = self
            .find_by_token(token)
            .ok()
            .flatten()
            .filter(|record| record.is_active(now_unix_seconds))?;
        let role = record.role_for_database(database)?;
        let _ = self.touch_last_used(&record.name, &record.token_id, now_unix_seconds);
        Some(role)
    }

    fn find_by_token(&self, token: &str) -> Result<Option<WebUserToken>, String> {
        let kv = self
            .kv
            .lock()
            .map_err(|_| "web token store lock poisoned".to_string())?;
        let Some(user_key) = web_token_digests(token)
            .iter()
            .find_map(|digest| kv.get(&web_token_digest_key(digest)).ok().flatten())
            .or_else(|| {
                kv.get(&web_token_lookup_key(token))
                    .map_err(|err| err.to_string())
                    .ok()
                    .flatten()
            })
        else {
            return Ok(None);
        };
        kv.get(&user_key)
            .map_err(|err| err.to_string())?
            .map(|value| decode_web_user_token(&value))
            .transpose()
    }

    pub(super) fn put(&self, record: WebUserToken) -> Result<(), String> {
        let mut kv = self
            .kv
            .lock()
            .map_err(|_| "web token store lock poisoned".to_string())?;
        let user_key = web_user_token_key(&record.name, &record.token_id);
        if let Some(old) = kv.get(&user_key).map_err(|err| err.to_string())? {
            let old = decode_web_user_token(&old)?;
            kv.delete(&web_token_digest_key(&old.token))
                .map_err(|err| err.to_string())?;
            kv.delete(&web_token_digest_lookup_key(&old.token))
                .map_err(|err| err.to_string())?;
            kv.delete(&web_token_lookup_key(&old.token))
                .map_err(|err| err.to_string())?;
        }
        let mut stored = record;
        stored.token = web_token_digest(&stored.token);
        kv.put(&user_key, encode_web_user_token(&stored).as_bytes())
            .map_err(|err| err.to_string())?;
        if !stored.revoked {
            kv.put(&web_token_digest_key(&stored.token), &user_key)
                .map_err(|err| err.to_string())?;
        }
        Ok(())
    }

    pub(super) fn revoke(&self, name: &str, token_id: &str) -> Result<(), String> {
        let mut kv = self
            .kv
            .lock()
            .map_err(|_| "web token store lock poisoned".to_string())?;
        let user_key = web_user_token_key(name, token_id);
        let Some(value) = kv.get(&user_key).map_err(|err| err.to_string())? else {
            return Err(format!("unknown token {name:?}/{token_id:?}"));
        };
        let mut record = decode_web_user_token(&value)?;
        record.revoked = true;
        kv.put(&user_key, encode_web_user_token(&record).as_bytes())
            .map_err(|err| err.to_string())?;
        kv.delete(&web_token_digest_key(&record.token))
            .map_err(|err| err.to_string())?;
        kv.delete(&web_token_digest_lookup_key(&record.token))
            .map_err(|err| err.to_string())?;
        kv.delete(&web_token_lookup_key(&record.token))
            .map_err(|err| err.to_string())?;
        Ok(())
    }

    fn touch_last_used(
        &self,
        name: &str,
        token_id: &str,
        now_unix_seconds: u128,
    ) -> Result<(), String> {
        let mut kv = self
            .kv
            .lock()
            .map_err(|_| "web token store lock poisoned".to_string())?;
        let user_key = web_user_token_key(name, token_id);
        let Some(value) = kv.get(&user_key).map_err(|err| err.to_string())? else {
            return Ok(());
        };
        let mut record = decode_web_user_token(&value)?;
        record.last_used_at = now_unix_seconds;
        kv.put(&user_key, encode_web_user_token(&record).as_bytes())
            .map_err(|err| err.to_string())
    }

    pub(super) fn revoke_database_tokens(&self, database: &str) -> Result<usize, String> {
        let mut kv = self
            .kv
            .lock()
            .map_err(|_| "web token store lock poisoned".to_string())?;
        let records = kv
            .scan_prefix(WEB_AUTH_USER_PREFIX)
            .map_err(|err| err.to_string())?;
        let mut revoked = 0_usize;
        for (key, value) in records {
            let mut record = decode_web_user_token(&value)?;
            if record.revoked || !record.database_roles.contains_key(database) {
                continue;
            }
            record.revoked = true;
            kv.put(&key, encode_web_user_token(&record).as_bytes())
                .map_err(|err| err.to_string())?;
            kv.delete(&web_token_digest_key(&record.token))
                .map_err(|err| err.to_string())?;
            kv.delete(&web_token_digest_lookup_key(&record.token))
                .map_err(|err| err.to_string())?;
            kv.delete(&web_token_lookup_key(&record.token))
                .map_err(|err| err.to_string())?;
            revoked += 1;
        }
        Ok(revoked)
    }

    pub(super) fn cleanup_expired(&self, now_unix_seconds: u128) -> Result<usize, String> {
        let mut kv = self
            .kv
            .lock()
            .map_err(|_| "web token store lock poisoned".to_string())?;
        let records = kv
            .scan_prefix(WEB_AUTH_USER_PREFIX)
            .map_err(|err| err.to_string())?;
        let mut removed = 0_usize;
        for (key, value) in records {
            let record = decode_web_user_token(&value)?;
            if record.expired_at == 0 || record.expired_at > now_unix_seconds {
                continue;
            }
            kv.delete(&web_token_digest_key(&record.token))
                .map_err(|err| err.to_string())?;
            kv.delete(&web_token_digest_lookup_key(&record.token))
                .map_err(|err| err.to_string())?;
            kv.delete(&web_token_lookup_key(&record.token))
                .map_err(|err| err.to_string())?;
            kv.delete(&key).map_err(|err| err.to_string())?;
            removed += 1;
        }
        Ok(removed)
    }

    pub(super) fn delete_user(&self, name: &str) -> Result<(), String> {
        let mut kv = self
            .kv
            .lock()
            .map_err(|_| "web token store lock poisoned".to_string())?;
        let records = kv
            .scan_prefix(&web_user_prefix(name))
            .map_err(|err| err.to_string())?;
        if records.is_empty() {
            return Err(format!("unknown web user {name:?}"));
        }
        for (key, value) in records {
            let record = decode_web_user_token(&value)?;
            kv.delete(&web_token_digest_key(&record.token))
                .map_err(|err| err.to_string())?;
            kv.delete(&web_token_digest_lookup_key(&record.token))
                .map_err(|err| err.to_string())?;
            kv.delete(&web_token_lookup_key(&record.token))
                .map_err(|err| err.to_string())?;
            kv.delete(&key).map_err(|err| err.to_string())?;
        }
        Ok(())
    }
}

impl WebSessionStore {
    pub(super) fn open(path: PathBuf) -> Result<Self, String> {
        ensure_store_parent(&path)?;
        RocksKvStore::open(path)
            .map(|kv| Self {
                kv: Arc::new(Mutex::new(kv)),
            })
            .map_err(|err| err.to_string())
    }

    pub(super) fn create(
        &self,
        token: &str,
        database: &str,
        role: WebRole,
        now: u128,
        ttl_seconds: u128,
    ) -> Result<WebSession, String> {
        let seed = format!("{token}:{database}:{now}:{}", unix_millis_now());
        let session_id = format!(
            "sid:{}",
            stable_keyed_digest_hex(b"neo4r-web-session-v1", seed.as_bytes())
        );
        let csrf_token = stable_keyed_digest_hex(b"neo4r-web-csrf-v1", session_id.as_bytes());
        let expires_at = now.saturating_add(ttl_seconds.max(1));
        let record = format!(
            "session_id={}\ndatabase={}\nrole={}\nexpires_at={}\ncsrf_token={}\n",
            session_id,
            database,
            role.as_str(),
            expires_at,
            csrf_token
        );
        let mut kv = self
            .kv
            .lock()
            .map_err(|_| "web session store lock poisoned".to_string())?;
        kv.put(&web_session_key(&session_id), record.as_bytes())
            .map_err(|err| err.to_string())?;
        Ok(WebSession {
            session_id,
            csrf_token,
        })
    }

    pub(super) fn role_for_session(
        &self,
        session_id: &str,
        database: &str,
        now: u128,
    ) -> Option<WebRole> {
        let record = self.session_record(session_id)?;
        if record.expires_at < now {
            return None;
        }
        if record.database != "*" && record.database != database {
            return None;
        }
        Some(record.role)
    }

    pub(super) fn csrf_for_session(&self, session_id: &str, now: u128) -> Option<String> {
        let record = self.session_record(session_id)?;
        if record.expires_at < now {
            return None;
        }
        Some(record.csrf_token)
    }

    pub(super) fn delete(&self, session_id: &str) -> Result<(), String> {
        let mut kv = self
            .kv
            .lock()
            .map_err(|_| "web session store lock poisoned".to_string())?;
        kv.delete(&web_session_key(session_id))
            .map_err(|err| err.to_string())
    }

    pub(super) fn cleanup_expired(&self, now: u128) -> Result<usize, String> {
        let mut kv = self
            .kv
            .lock()
            .map_err(|_| "web session store lock poisoned".to_string())?;
        let sessions = kv
            .scan_prefix(WEB_SESSION_PREFIX)
            .map_err(|err| err.to_string())?;
        let mut removed = 0_usize;
        for (key, value) in sessions {
            let record = decode_web_session_record(&value)?;
            if record.expires_at >= now {
                continue;
            }
            kv.delete(&key).map_err(|err| err.to_string())?;
            removed += 1;
        }
        Ok(removed)
    }

    fn session_record(&self, session_id: &str) -> Option<WebSessionRecord> {
        let kv = self.kv.lock().ok()?;
        let bytes = kv.get(&web_session_key(session_id)).ok().flatten()?;
        decode_web_session_record(&bytes).ok()
    }
}

fn ensure_store_parent(path: &std::path::Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WebSessionRecord {
    database: String,
    role: WebRole,
    expires_at: u128,
    csrf_token: String,
}

fn decode_web_session_record(input: &[u8]) -> Result<WebSessionRecord, String> {
    let mut stored_database = None;
    let mut role = None;
    let mut expires_at = None;
    let mut csrf_token = None;
    for line in String::from_utf8_lossy(input).lines() {
        if let Some(value) = line.strip_prefix("database=") {
            stored_database = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("role=") {
            role = Some(parse_web_role(value)?);
        } else if let Some(value) = line.strip_prefix("expires_at=") {
            expires_at = Some(value.parse::<u128>().map_err(|err| err.to_string())?);
        } else if let Some(value) = line.strip_prefix("csrf_token=") {
            csrf_token = Some(value.to_string());
        }
    }
    let database = stored_database.ok_or_else(|| "session database is missing".to_string())?;
    let role = role.ok_or_else(|| "session role is missing".to_string())?;
    let expires_at = expires_at.ok_or_else(|| "session expires_at is missing".to_string())?;
    let csrf_token = csrf_token.unwrap_or_else(|| {
        stable_keyed_digest_hex(
            b"neo4r-web-csrf-v1-legacy",
            format!("{database}:{expires_at}").as_bytes(),
        )
    });
    Ok(WebSessionRecord {
        database,
        role,
        expires_at,
        csrf_token,
    })
}

impl WebUserToken {
    pub(super) fn is_active(&self, now_unix_seconds: u128) -> bool {
        !self.revoked && (self.expired_at == 0 || self.expired_at > now_unix_seconds)
    }

    fn role_for_database(&self, database: &str) -> Option<WebRole> {
        if self.database_roles.is_empty() {
            return Some(self.role);
        }
        self.database_roles
            .get(database)
            .copied()
            .or_else(|| self.database_roles.get("*").copied())
    }
}

pub(super) fn parse_web_role(input: &str) -> Result<WebRole, String> {
    if input.eq_ignore_ascii_case("reader") {
        Ok(WebRole::Reader)
    } else if input.eq_ignore_ascii_case("writer") {
        Ok(WebRole::Writer)
    } else if input.eq_ignore_ascii_case("admin") {
        Ok(WebRole::Admin)
    } else {
        Err(format!("unsupported web role {input:?}"))
    }
}

pub(super) fn validate_web_user_name(name: &str) -> Result<(), String> {
    if name.trim().is_empty() || name.contains(['\t', '\n', '\r']) {
        return Err("web user name must be a non-empty single-line value".to_string());
    }
    Ok(())
}

pub(super) fn validate_web_token_id(token_id: &str) -> Result<(), String> {
    if token_id.trim().is_empty() || token_id.contains(['\t', '\n', '\r', '/']) {
        return Err("web token id must be a non-empty single-line value without '/'".to_string());
    }
    Ok(())
}

pub(super) fn validate_web_user_token(token: &str) -> Result<(), String> {
    if token.trim().is_empty() || token.contains(['\t', '\n', '\r']) {
        return Err("web user token must be a non-empty single-line value".to_string());
    }
    Ok(())
}

pub(super) fn parse_database_roles(input: &str) -> Result<BTreeMap<String, WebRole>, String> {
    let mut roles = BTreeMap::new();
    for part in input
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let (database, role) = part
            .split_once('=')
            .ok_or_else(|| format!("invalid database role entry {part:?}"))?;
        validate_database_scope(database)?;
        roles.insert(database.to_string(), parse_web_role(role.trim())?);
    }
    Ok(roles)
}

pub(super) fn format_database_roles(roles: &BTreeMap<String, WebRole>) -> String {
    roles
        .iter()
        .map(|(database, role)| format!("{database}={}", role.as_str()))
        .collect::<Vec<_>>()
        .join(",")
}

fn validate_database_scope(database: &str) -> Result<(), String> {
    if database == "*" {
        return Ok(());
    }
    crate::tenant::validate_database_name(database)
}

pub(super) fn unix_seconds_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as u128)
        .unwrap_or_default()
}

pub(super) fn unix_millis_now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

pub(super) fn web_token_digest(token: &str) -> String {
    stable_keyed_digest_hex(web_token_digest_primary_key(), token.as_bytes())
}

fn web_token_digests(token: &str) -> Vec<String> {
    vec![
        stable_keyed_digest_hex(web_token_digest_primary_key(), token.as_bytes()),
        stable_keyed_digest_hex(b"neo4r-web-token-v1", token.as_bytes()),
    ]
}

fn web_token_digest_primary_key() -> &'static [u8] {
    b"neo4r-web-token-v2"
}

fn stable_keyed_digest_hex(key: &[u8], value: &[u8]) -> String {
    let mut left = 0xcbf29ce484222325_u64;
    let mut right = 0x84222325cbf29ce4_u64;
    for byte in key.iter().chain(value.iter()) {
        left ^= *byte as u64;
        left = left.wrapping_mul(0x100000001b3);
        right ^= (*byte as u64).rotate_left(1);
        right = right.wrapping_mul(0x9e3779b185ebca87);
    }
    format!("{left:016x}{right:016x}")
}

pub(super) fn constant_time_token_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut diff = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        let left_byte = left.get(index).copied().unwrap_or_default();
        let right_byte = right.get(index).copied().unwrap_or_default();
        diff |= (left_byte ^ right_byte) as usize;
    }
    diff == 0
}

fn encode_web_user_token(record: &WebUserToken) -> String {
    format!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        record.name,
        record.token_id,
        record.role.as_str(),
        record.token,
        record.expired_at,
        record.revoked,
        format_database_roles(&record.database_roles),
        record.created_at,
        record.last_used_at
    )
}

fn decode_web_user_token(input: &[u8]) -> Result<WebUserToken, String> {
    let input = std::str::from_utf8(input).map_err(|err| err.to_string())?;
    let parts = input.split('\t').collect::<Vec<_>>();
    if parts.len() != 6 && parts.len() != 7 && parts.len() != 9 {
        return Err(format!("invalid web token record {input:?}"));
    }
    let database_roles_index = if parts.len() >= 7 { Some(6) } else { None };
    Ok(WebUserToken {
        name: parts[0].to_string(),
        token_id: parts[1].to_string(),
        role: parse_web_role(parts[2])?,
        token: parts[3].to_string(),
        expired_at: parts[4]
            .parse::<u128>()
            .map_err(|_| format!("invalid expired_at {:?}", parts[4]))?,
        revoked: parts[5]
            .parse::<bool>()
            .map_err(|_| format!("invalid revoked flag {:?}", parts[5]))?,
        database_roles: if let Some(index) = database_roles_index {
            parse_database_roles(parts[index])?
        } else {
            BTreeMap::new()
        },
        created_at: if parts.len() == 9 {
            parts[7]
                .parse::<u128>()
                .map_err(|_| format!("invalid created_at {:?}", parts[7]))?
        } else {
            0
        },
        last_used_at: if parts.len() == 9 {
            parts[8]
                .parse::<u128>()
                .map_err(|_| format!("invalid last_used_at {:?}", parts[8]))?
        } else {
            0
        },
    })
}

fn web_user_prefix(name: &str) -> Vec<u8> {
    let mut key = Vec::from(WEB_AUTH_USER_PREFIX);
    key.extend_from_slice(name.as_bytes());
    key.push(b'/');
    key
}

fn web_user_token_key(name: &str, token_id: &str) -> Vec<u8> {
    let mut key = web_user_prefix(name);
    key.extend_from_slice(token_id.as_bytes());
    key
}

fn web_token_lookup_key(token: &str) -> Vec<u8> {
    let mut key = Vec::from(WEB_AUTH_TOKEN_PREFIX);
    key.extend_from_slice(token.as_bytes());
    key
}

fn web_token_digest_lookup_key(token: &str) -> Vec<u8> {
    web_token_digest_key(&web_token_digest(token))
}

fn web_token_digest_key(digest: &str) -> Vec<u8> {
    let mut key = Vec::from(WEB_AUTH_TOKEN_PREFIX);
    key.extend_from_slice(digest.as_bytes());
    key
}

fn web_session_key(session_id: &str) -> Vec<u8> {
    let mut key = Vec::from(WEB_SESSION_PREFIX);
    key.extend_from_slice(session_id.as_bytes());
    key
}

fn web_audit_key(unix_ms: u128, action: &str, target: &str) -> Vec<u8> {
    let mut key = Vec::from(WEB_AUDIT_PREFIX);
    key.extend_from_slice(format!("{unix_ms:020}/{action}/{target}").as_bytes());
    key
}

fn encode_web_audit_event(event: &WebAuditEvent) -> String {
    format!(
        "{}\t{}\t{}\t{}\n",
        event.unix_ms,
        event.action.replace('\t', " "),
        event.target.replace('\t', " "),
        event.detail.replace('\t', " ")
    )
}

fn decode_web_audit_event(input: &[u8]) -> Result<WebAuditEvent, String> {
    let input = std::str::from_utf8(input).map_err(|err| err.to_string())?;
    let parts = input.trim_end().splitn(4, '\t').collect::<Vec<_>>();
    if parts.len() != 4 {
        return Err(format!("invalid web audit record {input:?}"));
    }
    Ok(WebAuditEvent {
        unix_ms: parts[0]
            .parse::<u128>()
            .map_err(|_| format!("invalid audit timestamp {:?}", parts[0]))?,
        action: parts[1].to_string(),
        target: parts[2].to_string(),
        detail: parts[3].to_string(),
    })
}

#[cfg(test)]
mod tests {
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
            decode_web_user_token(b"alice\tmain\twriter\tsecret\t0\tfalse\ttenant_a=reader")
                .unwrap();

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
}
