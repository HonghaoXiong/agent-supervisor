//! One-click attach integration tests — the app-level discovery state
//! machine against in-process scripted UDS servers, with the discovery
//! round INJECTED (deterministic: never reads the developer's real
//! `~/.czs/ui-registry.json` or CWD):
//!
//! * waiting  — no hub anywhere: honest waiting status, bounded ~3 s
//!              rescan schedule, no storm
//! * attach   — a hub appearing in a later round auto-attaches (no env
//!              var, no matching CWD), hub label shown
//! * cycle    — two live hubs: connect newest; cycle_hub() pins the
//!              next and the following round connects to it
//! * manual   — an explicit connect() pins retries to that socket,
//!              ignoring discovery entirely (legacy/test posture)
//! * vanish   — a registry entry whose socket died mid-round: waiting
//!              posture again, then recovery when discovery revives

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[path = "../src/ipc.rs"]
mod ipc;
#[path = "../src/main.rs"]
mod app;

use app::discovery::{DiscoveryOutcome, HubEntry};
use app::SupervisorApp;
use app::model::Health;

// ---------------------------------------------------------------- server

fn socket_path(name: &str) -> String {
    let dir = std::env::temp_dir()
        .join(format!("oneclick-attach-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("{name}.sock"))
        .to_str()
        .unwrap()
        .to_string()
}

const HELLO: &str =
    r#"{"ok":true,"v":1,"initial_snapshot_cursor":{"cut_seq":1}}"#;
const SNAPSHOT: &str = r#"{"ok":true,"v":1,"states":[],"done":true}"#;
const SNAPSHOT_EVENTS: &str =
    r#"{"ok":true,"v":1,"events":[],"done":true}"#;
const SUBACK: &str = r#"{"ok":true,"v":1,"subscribed":"*"}"#;

fn read_line(stream: &mut UnixStream) -> String {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut line = String::new();
    let _ = reader.read_line(&mut line);
    line
}

fn send(stream: &mut UnixStream, line: &str) {
    let _ = stream.write_all((line.to_string() + "\n").as_bytes());
    let _ = stream.flush();
}

/// The standard request-phase handshake (hello → snapshot →
/// snapshot-events → subscribe), tolerant of an early peer close.
fn handshake(stream: &mut UnixStream) {
    let mut hello_answered = false;
    loop {
        let line = read_line(stream);
        if line.is_empty() {
            return;
        }
        if !hello_answered {
            send(stream, HELLO);
            hello_answered = true;
            continue;
        }
        if line.contains("\"op\":\"snapshot-events\"") {
            send(stream, SNAPSHOT_EVENTS);
        } else if line.contains("\"op\":\"snapshot\"") {
            send(stream, SNAPSHOT);
        } else if line.contains("\"op\":\"subscribe\"") {
            send(stream, SUBACK);
            return;
        } else {
            return;
        }
    }
}

/// Bind + accept LOOP, handshake per connection, keep each connection
/// open (parked) so subscriptions stay live. Detached threads.
fn scripted_hub(name: &str) -> String {
    let path = socket_path(name);
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).expect("bind");
    std::thread::spawn(move || loop {
        if let Ok((mut stream, _)) = listener.accept() {
            std::thread::spawn(move || {
                handshake(&mut stream);
                // park until the peer goes away
                let mut buf = [0u8; 64];
                let _ = std::io::Read::read(&mut stream, &mut buf);
            });
        }
    });
    path
}

fn hub_entry(socket: &str, project: &str, started: f64) -> HubEntry {
    HubEntry {
        socket: socket.to_string(),
        pid: 1000 + started as i64,
        project_root: project.to_string(),
        started_at: started,
    }
}

/// Sequence-injected discovery: each call returns the next queued
/// outcome (repeating the LAST one when the script runs dry — a stable
/// world assumption for the tail of each test).
fn sequenced(
    outcomes: Vec<DiscoveryOutcome>,
) -> (Arc<Mutex<Vec<DiscoveryOutcome>>>,
      Box<dyn Fn() -> DiscoveryOutcome + Send>) {
    let script = Arc::new(Mutex::new(outcomes));
    let reader = {
        let script = Arc::clone(&script);
        Box::new(move || {
            let mut queue = script.lock().unwrap();
            if queue.len() > 1 {
                queue.remove(0)
            } else {
                queue[0].clone()
            }
        }) as Box<dyn Fn() -> DiscoveryOutcome + Send>
    };
    (script, reader)
}

fn due(app: &SupervisorApp) -> Instant {
    app.reconnect_at.expect("a reconnect must be scheduled")
}

// ---------------------------------------------------------------- tests

#[test]
fn no_hub_shows_waiting_with_bounded_rescan() {
    let (_, reader) = sequenced(vec![DiscoveryOutcome::None]);
    let mut app = SupervisorApp::default();
    app.discover_fn = Some(reader);
    // drive the app on an injected clock: each round may schedule at
    // most ~3 s ahead of ITS OWN round clock (the bounded cadence);
    // real time never advances, so due-times are compared to the
    // injected clock, not Instant::now()
    let mut clock = Instant::now();
    app.reconnect_round(clock);
    assert!(app.waiting);
    assert!(app.status_line.contains("waiting for a supervisor hub"));
    assert!(app.status_line.contains("auto-attach"));
    assert!(app.reconnect_at.is_some());
    for _ in 0..6 {
        let at = due(&app);
        let wait = at.saturating_duration_since(clock);
        assert!(
            wait <= Duration::from_millis(3001),
            "rescan storm: {wait:?}"
        );
        clock = at + Duration::from_millis(1);
        app.pump_with_clock(clock);
        assert!(app.waiting, "still nothing to attach to");
    }
}

/// polish1 N2: an EMPTY `Registry(vec![])` outcome (unreachable from
/// the real resolver, possible from an injected discover_fn) used to
/// panic on `live[index]`. Contract: empty candidates are "nothing
/// anywhere" — the same waiting/rescan path as `None`, never an index.
#[test]
fn empty_registry_outcome_waits_like_none_never_panics() {
    let (_, reader) = sequenced(vec![DiscoveryOutcome::Registry(vec![])]);
    let mut app = SupervisorApp::default();
    app.discover_fn = Some(reader);
    app.reconnect_round(Instant::now());
    assert!(app.waiting, "empty registry == nothing discovered");
    assert!(app.status_line.contains("waiting for a supervisor hub"));
    assert!(app.reconnect_at.is_some(), "bounded rescan scheduled");
    assert!(app.hub_candidates.is_empty());
    assert_eq!(app.hub_label, None);
    // the waiting posture keeps rescanning bounded (same as None)
    let at = due(&app);
    assert!(at.duration_since(Instant::now())
        <= Duration::from_millis(3001) + Duration::from_secs(1));
}

#[test]
fn hub_appearing_later_auto_attaches_with_label() {
    let hub = scripted_hub("attach");
    let (_, reader) = sequenced(vec![
        DiscoveryOutcome::None,
        DiscoveryOutcome::Registry(vec![
            hub_entry(&hub, "/Users/x/underworld", 9.0),
        ]),
    ]);
    let mut app = SupervisorApp::default();
    app.discover_fn = Some(reader);
    app.reconnect_round(Instant::now());
    assert!(app.waiting); // round 1: nothing yet
    app.pump_with_clock(due(&app) + Duration::from_millis(1));
    // round 2: the registry round found the hub and connected
    assert!(!app.waiting);
    assert_eq!(app.reducer.projection.health, Some(Health::Live));
    assert_eq!(app.hub_label.as_deref(), Some("underworld"));
    // polish1 single-source contract: the composed status bar carries
    // `hub:` exactly once (through the badge, not the status line)
    let bar = app::view::status_bar_text(
        &app.status_line,
        app.hub_label.as_deref(),
    );
    assert_eq!(bar.matches("hub:").count(), 1, "duplicate: {bar}");
    assert!(bar.contains("hub: underworld"));
    assert_eq!(app.reconnect_attempts, 0);
}

/// polish1: the user-screenshot defect — a single-instance registry
/// connect rendered `hub: <name>` TWICE (the status line embedded one
/// AND the badge widget drew another). The composed bar must carry
/// exactly one `hub:`, and the status line itself none.
#[test]
fn single_instance_status_bar_shows_exactly_one_hub_label() {
    let hub = scripted_hub("onehub");
    let (_, reader) = sequenced(vec![DiscoveryOutcome::Registry(vec![
        hub_entry(&hub, "/Users/x/czs-demo-proj", 9.0),
    ])]);
    let mut app = SupervisorApp::default();
    app.discover_fn = Some(reader);
    app.reconnect_round(Instant::now());
    assert!(!app.waiting);
    assert_eq!(app.hub_candidates.len(), 1, "single instance");
    assert!(
        !app.status_line.contains("hub:"),
        "status line must not embed the hub label: {}",
        app.status_line
    );
    let bar = app::view::status_bar_text(
        &app.status_line,
        app.hub_label.as_deref(),
    );
    assert_eq!(bar.matches("hub:").count(), 1, "duplicate: {bar}");
    assert!(bar.contains("connected: 0 tasks"));
    assert_eq!(
        bar.matches("hub: czs-demo-proj").count(),
        1,
        "the one badge names the hub: {bar}"
    );

    // multi instance: the SAME badge text (only the widget becomes the
    // cycle control — the text shape does not change)
    let other = scripted_hub("onehub-other");
    let (_, reader) = sequenced(vec![DiscoveryOutcome::Registry(vec![
        hub_entry(&hub, "/Users/x/czs-demo-proj", 9.0),
        hub_entry(&other, "/Users/x/czs-demo-proj-2", 1.0),
    ])]);
    let mut app = SupervisorApp::default();
    app.discover_fn = Some(reader);
    app.reconnect_round(Instant::now());
    assert_eq!(app.hub_candidates.len(), 2);
    let bar = app::view::status_bar_text(
        &app.status_line,
        app.hub_label.as_deref(),
    );
    assert_eq!(bar.matches("hub:").count(), 1, "multi duplicate: {bar}");
    assert!(bar.contains("hub: czs-demo-proj"));
}

#[test]
fn dead_registry_socket_keeps_waiting_then_recovers() {
    let hub = scripted_hub("revive");
    let (_, reader) = sequenced(vec![
        // entry points at a socket that never existed
        DiscoveryOutcome::Registry(vec![
            hub_entry("/nonexistent/oneclick.sock", "/p", 9.0),
        ]),
        DiscoveryOutcome::Registry(vec![
            hub_entry(&hub, "/Users/x/revive", 1.0),
        ]),
    ]);
    let mut app = SupervisorApp::default();
    app.discover_fn = Some(reader);
    app.reconnect_round(Instant::now());
    assert!(app.waiting, "failed attach keeps the auto-attach posture");
    assert!(app.status_line.contains("waiting"));
    app.pump_with_clock(due(&app) + Duration::from_millis(1));
    assert!(!app.waiting);
    assert_eq!(app.reducer.projection.health, Some(Health::Live));
}

#[test]
fn two_live_hubs_cycle_on_demand() {
    let older = scripted_hub("older");
    let newer = scripted_hub("newer");
    let live = vec![
        hub_entry(&newer, "/proj/newer", 9.0),
        hub_entry(&older, "/proj/older", 1.0),
    ];
    let (_, reader) =
        sequenced(vec![DiscoveryOutcome::Registry(live.clone())]);
    let mut app = SupervisorApp::default();
    app.discover_fn = Some(reader);
    app.reconnect_round(Instant::now());
    assert!(!app.waiting);
    assert_eq!(app.hub_label.as_deref(), Some("newer"), "newest wins");
    assert!(app.socket.ends_with("newer.sock"));
    assert_eq!(app.hub_candidates.len(), 2);

    // minimal switch: cycle pins the next hub and reconnects now
    app.cycle_hub();
    assert!(app.status_line.contains("switching"));
    app.pump_with_clock(due(&app) + Duration::from_millis(1));
    assert!(!app.waiting);
    assert_eq!(app.hub_label.as_deref(), Some("older"));
    assert!(app.socket.ends_with("older.sock"));

    // cycling wraps back to the newest
    app.cycle_hub();
    app.pump_with_clock(due(&app) + Duration::from_millis(1));
    assert_eq!(app.hub_label.as_deref(), Some("newer"));
}

#[test]
fn manual_connect_pins_retries_and_ignores_discovery() {
    let hub = scripted_hub("manual");
    let other = scripted_hub("other");
    let (_, reader) = sequenced(vec![DiscoveryOutcome::Registry(vec![
        hub_entry(&other, "/proj/other", 9.0),
    ])]);
    let mut app = SupervisorApp::default();
    app.discover_fn = Some(reader);
    app.connect(&hub).expect("manual connect");
    assert!(!app.waiting);
    // a voluntary disconnect schedule still retries the PINNED socket,
    // never the discovery result
    app.reconnect_at = Some(Instant::now());
    app.pump_with_clock(Instant::now() + Duration::from_millis(1));
    assert!(!app.waiting);
    assert!(app.socket.ends_with("manual.sock"));
    assert_eq!(app.hub_label, None);
}

#[test]
fn env_override_wins_and_never_shows_waiting() {
    let hub = scripted_hub("envwin");
    let (_, reader) = sequenced(vec![DiscoveryOutcome::Registry(vec![
        hub_entry("/nonexistent/other.sock", "/p", 9.0),
    ])]);
    let mut app = SupervisorApp::default();
    app.discover_fn = Some(reader);
    // env override is resolved OUTSIDE the injected closure by the real
    // discovery contract; here we assert the app honors an Env outcome
    // outright (the env precedence itself is covered in discovery's
    // unit tests)
    let (_, env_reader) = sequenced(vec![DiscoveryOutcome::Env(
        hub.clone(),
    )]);
    app.discover_fn = Some(env_reader);
    app.reconnect_round(Instant::now());
    assert!(!app.waiting);
    assert!(app.socket.ends_with("envwin.sock"));
    assert_eq!(app.hub_label, None, "env connections carry no hub label");
    assert!(app.hub_candidates.is_empty());
}
