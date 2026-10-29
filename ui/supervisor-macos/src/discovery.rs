//! Hub socket discovery for one-click attach (no env var, no matching
//! CWD required).
//!
//! Resolution order, first hit wins:
//!
//! 1. `CZS_UI_SOCKET` (absolute path) — explicit override, preserved
//!    verbatim from the pre-discovery behaviour;
//! 2. `~/.czs/ui-registry.json` (the Python hub's live-server registry,
//!    see `zcode_supervisor/ui_registry.py`): parse → drop entries whose
//!    socket does not exist or refuses a probe connect → newest
//!    `started_at` first;
//! 3. legacy CWD fallback `.czs/state/ui-events.sock` — only when the
//!    socket file exists (the old default layout).
//!
//! Liveness is decided by the SOCKET, not the pid: without a `libc`
//! dependency (kept at zero — `libc` is only a transitive dep in
//! Cargo.lock) there is no `kill(pid, 0)`, and a connect probe is the
//! ground truth the app needs anyway: a pid that is alive with a dead
//! listener must not be chosen. The probe connects and immediately
//! drops; the hub's per-connection cleanup handles that as a plain
//! peer disconnect.
//!
//! Pure, unit-testable core (`parse_registry`, `live_entries`,
//! `resolve`, `choose_index`, `next_index`, `rescan_backoff`) + one
//! thin IO wrapper (`discover`). Read-only: nothing here spawns
//! processes or writes files.

use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;

/// One registry entry (subset the app needs; the file may carry more).
#[derive(Debug, Clone, PartialEq)]
pub struct HubEntry {
    pub socket: String,
    pub pid: i64,
    pub project_root: String,
    pub started_at: f64,
}

/// The result of one full resolution round.
#[derive(Debug, Clone, PartialEq)]
pub enum DiscoveryOutcome {
    /// `CZS_UI_SOCKET` was set — an outright override (never probed).
    Env(String),
    /// Live registry entries, newest first. The caller picks (index 0
    /// by default, or a preferred socket for hub cycling).
    Registry(Vec<HubEntry>),
    /// Legacy CWD-relative socket exists.
    Fallback(String),
    /// Nothing anywhere: the app shows "waiting" and rescans bounded.
    None,
}

/// Legacy default layout (P1.1): relative to the app's CWD.
pub const DEFAULT_SOCKET: &str = ".czs/state/ui-events.sock";
/// Registry override hook, shared with the Python tests (same env var).
pub const REGISTRY_ENV: &str = "CZS_UI_REGISTRY";
/// The registry file name under `$HOME/.czs/`.
pub const REGISTRY_RELATIVE: &str = ".czs/ui-registry.json";

// ------------------------------------------------------------- pure core

/// Parse the registry file body. Tolerant by contract: bad JSON → no
/// entries; malformed individual entries (wrong types / missing socket)
/// are skipped, the healthy ones survive.
pub fn parse_registry(bytes: &[u8]) -> Vec<HubEntry> {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes)
    else {
        return Vec::new();
    };
    let Some(object) = value.as_object() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in object.values() {
        let Some(socket) = entry.get("socket").and_then(|v| v.as_str())
        else {
            continue;
        };
        if socket.is_empty() {
            continue;
        }
        let project_root = entry
            .get("project_root")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let started_at = entry
            .get("started_at")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let pid = entry.get("pid").and_then(|v| v.as_i64()).unwrap_or(0);
        out.push(HubEntry {
            socket: socket.to_string(),
            pid,
            project_root,
            started_at,
        });
    }
    out
}

/// Keep the entries the caller's liveness probe accepts, newest
/// `started_at` first (stable for equal timestamps).
pub fn live_entries(
    entries: Vec<HubEntry>,
    is_live: &dyn Fn(&HubEntry) -> bool,
) -> Vec<HubEntry> {
    let mut live: Vec<HubEntry> =
        entries.into_iter().filter(|e| is_live(e)).collect();
    live.sort_by(|a, b| {
        b.started_at
            .partial_cmp(&a.started_at)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    live
}

/// The full resolution order as one pure function. `cwd_socket` is
/// `Some(path)` only when the legacy CWD-relative socket file exists.
pub fn resolve(
    env_socket: Option<&str>,
    entries: Vec<HubEntry>,
    is_live: &dyn Fn(&HubEntry) -> bool,
    cwd_socket: Option<&str>,
) -> DiscoveryOutcome {
    if let Some(path) = env_socket.filter(|s| !s.is_empty()) {
        return DiscoveryOutcome::Env(path.to_string());
    }
    let live = live_entries(entries, is_live);
    if !live.is_empty() {
        return DiscoveryOutcome::Registry(live);
    }
    match cwd_socket.filter(|s| !s.is_empty()) {
        Some(path) => DiscoveryOutcome::Fallback(path.to_string()),
        None => DiscoveryOutcome::None,
    }
}

/// Which live entry to connect to: the preferred socket when it is
/// still among the live ones (hub cycling), else the newest (index 0).
pub fn choose_index(
    live: &[HubEntry],
    preferred: Option<&str>,
) -> usize {
    preferred
        .and_then(|p| live.iter().position(|e| e.socket == p))
        .unwrap_or(0)
}

/// The next index when cycling hubs: step past the currently connected
/// socket, wrapping; unknown current socket restarts at the newest.
pub fn next_index(live: &[HubEntry], current_socket: &str) -> usize {
    if live.is_empty() {
        return 0;
    }
    match live.iter().position(|e| e.socket == current_socket) {
        Some(index) => (index + 1) % live.len(),
        None => 0,
    }
}

/// Display name of a hub: the project directory's basename (`None` for
/// empty/`/` roots — the status line then just omits the label).
pub fn project_name(project_root: &str) -> Option<String> {
    std::path::Path::new(project_root)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .filter(|name| !name.is_empty())
}

/// Bounded rescan cadence while waiting for a hub: quick first retry,
/// capped at ~3 s so a missing hub can never become a scan storm.
pub fn rescan_backoff(attempt: usize) -> Duration {
    let ms = (1000 + attempt.min(4) as u64 * 500).min(3000);
    Duration::from_millis(ms)
}

// ------------------------------------------------------------ thin IO

/// The registry file location: `CZS_UI_REGISTRY` override, else
/// `$HOME/.czs/ui-registry.json` (`None` when HOME is unset).
pub fn registry_file() -> Option<PathBuf> {
    if let Ok(override_path) = std::env::var(REGISTRY_ENV) {
        let trimmed = override_path.trim().to_string();
        if !trimmed.is_empty() {
            return Some(PathBuf::from(trimmed));
        }
    }
    std::env::var("HOME").ok().filter(|h| !h.is_empty()).map(|home| {
        let mut path = PathBuf::from(home);
        path.push(REGISTRY_RELATIVE);
        path
    })
}

/// Socket-level liveness: the socket file exists AND a probe connect
/// is accepted (a live pid with a dead listener is NOT live for us).
/// The probe connection is dropped immediately — the hub treats it as
/// a plain peer disconnect.
pub fn probe_live(entry: &HubEntry) -> bool {
    UnixStream::connect(&entry.socket).is_ok()
}

/// One full discovery round against the real environment (the only IO
/// in this module). Never panics; any unreadable registry simply reads
/// as no entries.
pub fn discover() -> DiscoveryOutcome {
    let env_socket = std::env::var("CZS_UI_SOCKET")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let entries = registry_file()
        .and_then(|path| std::fs::read(path).ok())
        .map(|bytes| parse_registry(&bytes))
        .unwrap_or_default();
    let cwd_socket =
        Some(DEFAULT_SOCKET).filter(|p| std::path::Path::new(p).exists());
    resolve(
        env_socket.as_deref(),
        entries,
        &probe_live,
        cwd_socket,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(socket: &str, started: f64) -> HubEntry {
        HubEntry {
            socket: socket.into(),
            pid: 1000 + started as i64,
            project_root: format!("/proj/{started}"),
            started_at: started,
        }
    }

    fn always_live(_: &HubEntry) -> bool {
        true
    }

    fn never_live(_: &HubEntry) -> bool {
        false
    }

    #[test]
    fn parse_registry_multi_instance_and_malformed_tolerated() {
        let body = format!(
            "{{\"1\":{{\"socket\":\"/a\",\"pid\":1,\
              \"project_root\":\"/pa\",\"started_at\":1.0}},\
              \"2\":{{\"socket\":\"/b\",\"pid\":2,\
              \"project_root\":\"/pb\",\"started_at\":2.5}},\
              \"junk\":\"not-an-object\",\
              \"nosocket\":{{\"pid\":3,\"started_at\":9.0}},\
              \"emptysocket\":{{\"socket\":\"\",\"pid\":4}}}}"
        );
        let entries = parse_registry(body.as_bytes());
        assert_eq!(entries.len(), 2, "malformed entries skipped");
        assert!(entries.iter().any(|e| e.socket == "/a"));
        assert!(entries.iter().any(|e| e.socket == "/b"));
    }

    #[test]
    fn parse_registry_bad_json_is_empty_not_panic() {
        assert!(parse_registry(b"{ this is not json").is_empty());
        assert!(parse_registry(b"[1,2,3]").is_empty());
        assert!(parse_registry(b"").is_empty());
    }

    #[test]
    fn live_entries_sorted_newest_first() {
        let entries =
            vec![entry("/old", 1.0), entry("/new", 9.0), entry(
                "/mid", 4.0)];
        let live = live_entries(entries, &always_live);
        assert_eq!(
            live.iter().map(|e| e.socket.as_str()).collect::<Vec<_>>(),
            vec!["/new", "/mid", "/old"]
        );
    }

    #[test]
    fn live_entries_dead_filtered() {
        let entries =
            vec![entry("/dead", 9.0), entry("/live", 1.0), entry(
                "/alsodead", 5.0)];
        let live = live_entries(
            entries,
            &|e| e.socket == "/live",
        );
        assert_eq!(
            live.iter().map(|e| e.socket.as_str()).collect::<Vec<_>>(),
            vec!["/live"]
        );
    }

    #[test]
    fn resolve_env_overrides_everything() {
        let outcome = resolve(
            Some("/env.sock"),
            vec![entry("/hub", 1.0)],
            &always_live,
            Some(".czs/state/ui-events.sock"),
        );
        assert_eq!(outcome, DiscoveryOutcome::Env("/env.sock".into()));
        // an empty env value is treated as unset
        let outcome = resolve(
            Some(""),
            vec![],
            &always_live,
            Some(".czs/state/ui-events.sock"),
        );
        assert_eq!(
            outcome,
            DiscoveryOutcome::Fallback(".czs/state/ui-events.sock".into())
        );
    }

    #[test]
    fn resolve_registry_before_cwd_fallback() {
        let outcome = resolve(
            None,
            vec![entry("/hub", 1.0)],
            &always_live,
            Some(".czs/state/ui-events.sock"),
        );
        assert_eq!(
            outcome,
            DiscoveryOutcome::Registry(vec![entry("/hub", 1.0)])
        );
    }

    #[test]
    fn resolve_cwd_fallback_only_when_present() {
        let outcome = resolve(None, vec![], &always_live, Some("/cwd/s"));
        assert_eq!(outcome, DiscoveryOutcome::Fallback("/cwd/s".into()));
        let outcome = resolve(None, vec![entry("/x", 1.0)], &never_live, None);
        assert_eq!(outcome, DiscoveryOutcome::None);
        // dead registry entries do NOT block the cwd fallback
        let outcome = resolve(
            None,
            vec![entry("/x", 1.0)],
            &never_live,
            Some("/cwd/s"),
        );
        assert_eq!(outcome, DiscoveryOutcome::Fallback("/cwd/s".into()));
    }

    #[test]
    fn choose_and_cycle_indices() {
        let live = vec![entry("/newest", 9.0), entry("/older", 1.0)];
        assert_eq!(choose_index(&live, None), 0);
        assert_eq!(choose_index(&live, Some("/older")), 1);
        assert_eq!(choose_index(&live, Some("/gone")), 0);
        assert_eq!(next_index(&live, "/newest"), 1);
        assert_eq!(next_index(&live, "/older"), 0, "wraps");
        assert_eq!(next_index(&live, "/unknown"), 0);
        assert_eq!(next_index(&[], "/whatever"), 0);
    }

    #[test]
    fn project_name_is_basename_only() {
        assert_eq!(project_name("/Users/x/underworld"), Some("underworld".into()));
        assert_eq!(project_name("/"), None);
        assert_eq!(project_name(""), None);
        assert_eq!(project_name("relative/dir"), Some("dir".into()));
    }

    #[test]
    fn rescan_backoff_bounded_at_three_seconds() {
        assert_eq!(rescan_backoff(0), Duration::from_millis(1000));
        assert!(rescan_backoff(1) > rescan_backoff(0));
        assert_eq!(rescan_backoff(4), Duration::from_secs(3));
        assert_eq!(rescan_backoff(1000), Duration::from_secs(3));
    }

    #[test]
    fn registry_file_honors_override_and_home() {
        // override wins when non-empty (trimmed). env mutation is
        // process-wide; this binary's tests run in one process, so
        // set/clear sequentially and assert both states.
        std::env::set_var(REGISTRY_ENV, " /tmp/alt.json ");
        assert_eq!(registry_file(), Some(PathBuf::from("/tmp/alt.json")));
        std::env::remove_var(REGISTRY_ENV);
        let resolved = registry_file();
        if let Some(path) = resolved {
            assert!(path.ends_with(".czs/ui-registry.json"));
        }
    }
}
