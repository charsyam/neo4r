#[cfg(feature = "rdma")]
use neo4r_db::{
    RdmaProbeOptions, RdmaReplicationChannel, ReplicationEndpoint, SystemRdmaReplicationProvider,
};
#[cfg(feature = "rdma")]
use std::sync::Arc;
#[cfg(feature = "rdma")]
use std::time::Duration;

#[cfg(feature = "rdma")]
fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        eprintln!(
            "usage: cargo run -p neo4r-db --features rdma --example rdma_probe -- rdma://HOST:PORT [--source ADDR] [--count N] [--size BYTES] [--timeout-ms MS]"
        );
        std::process::exit(2);
    }

    let endpoint = args[0].clone();
    let mut source_addr = None;
    let mut ping_count = 3;
    let mut payload_size = 64;
    let mut timeout = Duration::from_secs(5);
    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--source" => {
                index += 1;
                source_addr = args.get(index).cloned();
            }
            "--count" => {
                index += 1;
                ping_count = parse_arg(&args, index, "--count");
            }
            "--size" => {
                index += 1;
                payload_size = parse_arg(&args, index, "--size");
            }
            "--timeout-ms" => {
                index += 1;
                let timeout_ms = parse_arg::<u64>(&args, index, "--timeout-ms");
                timeout = Duration::from_millis(timeout_ms);
            }
            unknown => {
                eprintln!("unknown argument: {unknown}");
                std::process::exit(2);
            }
        }
        index += 1;
    }

    let provider = SystemRdmaReplicationProvider::new(RdmaProbeOptions {
        source_addr,
        ping_count,
        payload_size,
        port: None,
        timeout,
    });
    let channel = RdmaReplicationChannel::new(Arc::new(provider));
    let report = channel
        .probe_endpoint(&ReplicationEndpoint::rdma(endpoint))
        .unwrap_or_else(|err| {
            eprintln!("{err}");
            std::process::exit(1);
        });

    println!(
        "provider={} target={}:{} count={} size={} elapsed_ms={}",
        report.provider,
        report.target_addr,
        report.port,
        report.ping_count,
        report.payload_size,
        report.elapsed_millis
    );
    if !report.stdout.trim().is_empty() {
        println!("{}", report.stdout.trim());
    }
    if !report.stderr.trim().is_empty() {
        eprintln!("{}", report.stderr.trim());
    }
}

#[cfg(not(feature = "rdma"))]
fn main() {
    eprintln!("rdma_probe requires: cargo run -p neo4r-db --features rdma --example rdma_probe");
    std::process::exit(2);
}

#[cfg(feature = "rdma")]
fn parse_arg<T>(args: &[String], index: usize, name: &str) -> T
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let value = args.get(index).unwrap_or_else(|| {
        eprintln!("{name} needs a value");
        std::process::exit(2);
    });
    value.parse::<T>().unwrap_or_else(|err| {
        eprintln!("invalid {name} value {value}: {err}");
        std::process::exit(2);
    })
}
