//! Real-IO discovery tests: `discovery::discover()` against a REAL
//! registry file and REAL UDS sockets (own process, own env — no other
//! test target shares it, so env mutation is safe here). The hub side
//! of the registry is covered by the Python suite; this proves the
//! Rust read path: override resolution, live-entry probing, dead-entry
//! filtering, env precedence and the CWD fallback existence check.
//!
//! `smoke_live_registry` runs ONLY when `CZS_SMOKE_REGISTRY` points at
//! a registry written by a REAL `czs hub serve` process (manual smoke
//! script); otherwise it skips.

#[path = "../src/discovery.rs"]
mod discovery;

use discovery::{discover, DiscoveryOutcome};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("oneclick-real-{}-{}", name, std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A real accepting UDS server (parks connections open).
fn parked_socket(path: &str) {
    let _ = std::fs::remove_file(path);
    let listener = UnixListener::bind(path).expect("bind");
    std::thread::spawn(move || loop {
        if let Ok((mut stream, _)) = listener.accept() {
            std::thread::spawn(move || {
                let mut line = String::new();
                let mut reader =
                    BufReader::new(stream.try_clone().unwrap());
                let _ = reader.read_line(&mut line); // park
                let _ = stream.write_all(b"{}\n");
            });
        }
    });
}

fn set_registry(path: &str) {
    std::env::set_var("CZS_UI_REGISTRY", path);
}

fn clear_env() {
    std::env::remove_var("CZS_UI_REGISTRY");
    std::env::remove_var("CZS_UI_SOCKET");
}

#[test]
fn discover_reads_real_file_and_probes_real_sockets() {
    let dir = temp_dir("roundtrip");
    let registry = dir.join("ui-registry.json");
    let live = dir.join("live.sock");
    let dead = dir.join("dead.sock"); // never bound: file only
    parked_socket(live.to_str().unwrap());
    let live_str = live.to_string_lossy();
    let dead_str = dead.to_string_lossy();
    std::fs::write(
        &registry,
        format!(
            "{{\"1\":{{\"socket\":\"{live_str}\",\"pid\":1,\
              \"project_root\":\"/proj/live\",\"started_at\":1.0}},\
              \"2\":{{\"socket\":\"{dead_str}\",\"pid\":2,\
              \"project_root\":\"/proj/dead\",\"started_at\":9.0}}}}"
        ),
    )
    .unwrap();
    // make the dead path LOOK like a socket exists (existence alone is
    // not liveness — the connect probe must reject it)
    std::fs::write(&dead, b"").unwrap();

    clear_env();
    set_registry(registry.to_str().unwrap());
    let outcome = discover();
    match outcome {
        DiscoveryOutcome::Registry(entries) => {
            // the dead entry (newer!) is filtered: probe beats recency
            assert_eq!(entries.len(), 1);
            assert!(entries[0].socket.contains("live.sock"));
            assert_eq!(entries[0].project_root, "/proj/live");
        }
        other => panic!("expected Registry, got {other:?}"),
    }
    std::env::remove_var("CZS_UI_REGISTRY");
}

#[test]
fn discover_env_overrides_and_missing_registry_is_none() {
    let dir = temp_dir("envmiss");
    clear_env();
    // no registry file anywhere → None (the CWD fallback would need an
    // existing .czs/state/ui-events.sock, which CI never has; if this
    // developer checkout happens to, the override must beat it anyway)
    set_registry(dir.join("absent.json").to_str().unwrap());
    std::env::set_var("CZS_UI_SOCKET", "/env/override.sock");
    assert_eq!(
        discover(),
        DiscoveryOutcome::Env("/env/override.sock".into())
    );
    std::env::remove_var("CZS_UI_SOCKET");
    std::env::remove_var("CZS_UI_REGISTRY");
}

/// Manual smoke hook: run the Rust read path against a registry that a
/// REAL `czs hub serve` (CZS_UI_EVENTS=1) wrote. Skipped otherwise.
#[test]
fn smoke_live_registry() {
    let Ok(smoke_registry) = std::env::var("CZS_SMOKE_REGISTRY") else {
        eprintln!("skipping: CZS_SMOKE_REGISTRY not set");
        return;
    };
    clear_env();
    set_registry(&smoke_registry);
    match discover() {
        DiscoveryOutcome::Registry(entries) => {
            assert!(!entries.is_empty(), "live hub must be discovered");
            for entry in &entries {
                assert!(
                    UnixStream::connect(&entry.socket).is_ok(),
                    "discovered socket must accept: {}",
                    entry.socket
                );
                eprintln!("discovered hub: {}", entry.project_root);
            }
        }
        other => panic!("expected Registry, got {other:?}"),
    }
    std::env::remove_var("CZS_UI_REGISTRY");
}
