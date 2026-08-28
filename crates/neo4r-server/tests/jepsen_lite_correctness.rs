use std::path::PathBuf;
use std::process::Command;

#[test]
fn jepsen_lite_correctness_runner_is_ci_invokable() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("server crate should live under crates/neo4r-server")
        .to_path_buf();
    let script = repo.join("scripts/jepsen-lite-correctness.sh");
    assert!(script.is_file(), "missing {}", script.display());

    let syntax = Command::new("bash")
        .arg("-n")
        .arg(&script)
        .status()
        .expect("bash should be available for smoke syntax check");
    assert!(syntax.success());

    if std::env::var("NEO4R_RUN_JEPSEN_LITE").ok().as_deref() != Some("1") {
        return;
    }

    let status = Command::new(&script)
        .status()
        .expect("jepsen-lite correctness runner should execute");
    assert!(status.success());
}
