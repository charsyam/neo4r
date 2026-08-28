use std::path::PathBuf;
use std::process::Command;

#[test]
fn multi_process_cluster_smoke_runner_is_ci_invokable() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("server crate should live under crates/neo4r-server")
        .to_path_buf();
    let script = repo.join("scripts/multi_process_cluster_smoke.sh");
    assert!(script.is_file(), "missing {}", script.display());

    let syntax = Command::new("bash")
        .arg("-n")
        .arg(&script)
        .status()
        .expect("bash should be available for smoke syntax check");
    assert!(syntax.success());

    if std::env::var("NEO4R_RUN_CLUSTER_SMOKE").ok().as_deref() != Some("1") {
        return;
    }

    let status = Command::new(&script)
        .status()
        .expect("cluster smoke runner should execute");
    assert!(status.success());
}
