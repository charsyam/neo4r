#[path = "main/runtime.rs"]
mod runtime;

fn main() {
    if let Err(err) = runtime::run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
