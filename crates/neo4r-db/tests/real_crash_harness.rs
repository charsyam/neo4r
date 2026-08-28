use neo4r_db::{DatabaseConfig, Neo4rDatabaseHandle};
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[test]
fn real_crash_child_writer() {
    let Ok(dir) = std::env::var("NEO4R_REAL_CRASH_CHILD_DIR") else {
        return;
    };
    let db = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    db.execute_cypher(r#"CREATE (n:Crash {name: "durable-before-kill"}) RETURN n"#)
        .unwrap();
    fs::write(PathBuf::from(&dir).join("child-ready"), b"ready").unwrap();
    thread::sleep(Duration::from_secs(30));
}

#[test]
fn real_crash_harness_reopens_child_written_data_after_kill() {
    if std::env::var("NEO4R_REAL_CRASH_CHILD_DIR").is_ok() {
        return;
    }
    let dir = temp_dir("neo4r-real-crash-harness");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("real_crash_child_writer")
        .arg("--exact")
        .arg("--nocapture")
        .env("NEO4R_REAL_CRASH_CHILD_DIR", &dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();

    let ready = dir.join("child-ready");
    let deadline = SystemTime::now() + Duration::from_secs(5);
    while !ready.is_file() {
        assert!(SystemTime::now() < deadline, "child did not write test row");
        thread::sleep(Duration::from_millis(50));
    }

    child.kill().unwrap();
    let _ = child.wait();

    let reopened = Neo4rDatabaseHandle::open(DatabaseConfig::new(&dir, 1, 1)).unwrap();
    let rows = reopened
        .execute_cypher(r#"MATCH (n:Crash) WHERE n.name = "durable-before-kill" RETURN n.name"#)
        .unwrap();
    assert_eq!(rows.len(), 1);

    let _ = fs::remove_dir_all(dir);
}

fn temp_dir(prefix: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "{}-{}",
        prefix,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
