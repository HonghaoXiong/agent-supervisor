//! P1.2 repair1 read-end lifecycle tests — the dispatch-mandated Rust
//! coverage against in-process scripted UDS servers:
//!
//! * overflow   — flood > CHANNEL_BOUND while the consumer sleeps:
//!                EXACTLY 1024 events + exactly ONE Reset + Disconnected
//! * disconnect — server drop → stale; then the SupervisorApp bounded
//!                reconnect path re-baselines on a second connection
//!                (Live again, old projection DISCARDED)
//! * halfline   — truncated final line: visible Reset marker, never
//!                silent loss
//! * oversize   — a line over MAX_LINE_BYTES: protocol error, bounded
//!                memory, marker delivered, reader stops
//! * version    — hello v=2: SupervisorApp refuses (stale); envelope
//!                v=2 frames refused by the reducer + stale
//! * coalesce   — subscribe ack + events in ONE write survive the
//!                single persistent reader

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::mpsc::TryRecvError;
use std::time::{Duration, Instant};

#[path = "../src/ipc.rs"]
mod ipc;
#[path = "../src/main.rs"]
mod app;

use app::SupervisorApp;
use app::model::{Health, Reducer};
use ipc::{Frame, UiClient, CHANNEL_BOUND};

// ---------------------------------------------------------------- server

fn socket_path(name: &str) -> String {
    let dir = std::env::temp_dir()
        .join(format!("p12-read-lifecycle-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("{name}.sock"))
        .to_str()
        .unwrap()
        .to_string()
}

/// Wait for the socket FILE to exist — bind() creates it before any
/// accept; we must never probe-connect (that would steal the accept).
fn wait_bound(path: &str, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if std::fs::metadata(path).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("server never bound {path}");
}

const HELLO: &str =
    r#"{"ok":true,"v":1,"initial_snapshot_cursor":{"cut_seq":1}}"#;
const SNAPSHOT: &str = r#"{"ok":true,"v":1,"states":[],"done":true}"#;
const SNAPSHOT_EVENTS: &str =
    r#"{"ok":true,"v":1,"events":[],"done":true}"#;
const SUBACK: &str = r#"{"ok":true,"v":1,"subscribed":"*"}"#;

/// Read one request line. The client is strictly ping-pong (one request
/// then waits), so a fresh reader per call cannot lose read-ahead.
/// Returns "" on EOF (peer gave up — e.g. a refused v=2 hello).
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

/// Answer the client request phase generically: read request lines and
/// dispatch on `op` until subscribe is answered. Works for BOTH flows
/// — the app (hello, snapshot, snapshot-events, subscribe) and the raw
/// client (hello, subscribe). `hello_v` overrides the hello version
/// (v≠1 scenario). Tolerates an early peer close (returns quietly).
fn handshake(stream: &mut UnixStream, hello_v: i64) {
    let mut hello_answered = false;
    loop {
        let line = read_line(stream);
        if line.is_empty() {
            return; // peer gave up (e.g. a refused v=2 hello)
        }
        if !hello_answered {
            let hello = if hello_v == 1 {
                HELLO.to_string()
            } else {
                HELLO.replace(r#""v":1"#, &format!(r#""v":{hello_v}"#))
            };
            send(stream, &hello);
            hello_answered = true;
            continue;
        }
        if line.contains("\"op\":\"snapshot-events\"") {
            send(stream, SNAPSHOT_EVENTS);
        } else if line.contains("\"op\":\"snapshot\"") {
            send(stream, SNAPSHOT);
        } else if line.contains("\"op\":\"subscribe\"") {
            send(stream, SUBACK);
            return; // request phase complete
        } else {
            return; // unknown op: nothing sane to answer
        }
    }
}

fn event_line(i: usize) -> String {
    format!(
        "{{\"type\":\"event\",\"event\":{{\"v\":1,\"event_id\":\"e/{i}\",\
         \"stream_id\":\"s\",\"seq\":{i},\"kind\":\"tool.call\",\
         \"task_id\":null}}}}\n"
    )
}

/// Bind + accept in a LOOP, running the script per connection (a
/// refused first client — e.g. version mismatch — must not kill the
/// server for the next one). Server threads are detached.
fn scripted_server<F>(name: &str, script: F) -> String
where
    F: Fn(&mut UnixStream) + Send + Sync + 'static,
{
    let path = socket_path(name);
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).expect("bind");
    let script = std::sync::Arc::new(script);
    std::thread::spawn(move || loop {
        if let Ok((mut stream, _)) = listener.accept() {
            let script = script.clone();
            std::thread::spawn(move || script(&mut stream));
        }
    });
    path
}

fn drain_until(
    receiver: &std::sync::mpsc::Receiver<Frame>,
    timeout: Duration,
) -> (Vec<Frame>, bool) {
    let start = Instant::now();
    let mut frames = Vec::new();
    let mut disconnected = false;
    while start.elapsed() < timeout {
        match receiver.try_recv() {
            Ok(f) => frames.push(f),
            Err(TryRecvError::Disconnected) => {
                disconnected = true;
                break;
            }
            Err(TryRecvError::Empty) => {
                std::thread::sleep(Duration::from_millis(10))
            }
        }
    }
    (frames, disconnected)
}

fn counts(frames: &[Frame]) -> (usize, usize) {
    let events =
        frames.iter().filter(|f| matches!(f, Frame::Event(_))).count();
    let resets = frames.iter().filter(|f| matches!(f, Frame::Reset)).count();
    (events, resets)
}

// ---------------------------------------------------------------- tests

#[test]
fn overflow_delivers_exactly_one_reset_then_disconnects() {
    let path = scripted_server("overflow", |stream| {
        handshake(stream, 1);
        let mut blob = String::new();
        for i in 1..=3000usize {
            blob.push_str(&event_line(i));
        }
        // keep the connection OPEN: any reader death is the client's
        // own overflow path, not an EOF
        let _ = stream.write_all(blob.as_bytes());
        let _ = stream.flush();
        std::thread::sleep(Duration::from_secs(2));
    });
    wait_bound(&path, Duration::from_secs(5));
    let (mut client, hello) = UiClient::connect(&path).expect("connect");
    let cursor = hello["initial_snapshot_cursor"].clone();
    let (receiver, ack) = client.subscribe_all(&cursor).expect("subscribe");
    assert_eq!(ack["ok"], serde_json::Value::Bool(true));

    // SLOW consumer: sleep while the flood lands, then drain
    std::thread::sleep(Duration::from_millis(300));
    let (frames, disconnected) =
        drain_until(&receiver, Duration::from_secs(3));
    let (events, resets) = counts(&frames);
    assert_eq!(events, CHANNEL_BOUND, "exactly the bound queued");
    assert_eq!(resets, 1, "exactly ONE guaranteed Reset marker");
    assert!(disconnected, "reader stopped — channel closes");
    // nothing further ever arrives (the reader is provably dead)
    std::thread::sleep(Duration::from_millis(100));
    assert!(receiver.try_recv().is_err());
    drop(client);
}

#[test]
fn halfline_eof_is_an_observable_marker() {
    let path = scripted_server("halfline", |stream| {
        handshake(stream, 1);
        let mut blob = String::new();
        for i in 1..=5usize {
            blob.push_str(&event_line(i));
        }
        blob.push_str(r#"{"type":"event","event":{"v":1,"event_id":"trun"#);
        let _ = stream.write_all(blob.as_bytes());
        let _ = stream.flush();
        std::thread::sleep(Duration::from_millis(100));
        let _ = stream.shutdown(std::net::Shutdown::Write);
        std::thread::sleep(Duration::from_millis(200));
    });
    wait_bound(&path, Duration::from_secs(5));
    let (mut client, hello) = UiClient::connect(&path).expect("connect");
    let cursor = hello["initial_snapshot_cursor"].clone();
    let (receiver, _) = client.subscribe_all(&cursor).expect("subscribe");
    let (frames, disconnected) =
        drain_until(&receiver, Duration::from_secs(3));
    let (events, resets) = counts(&frames);
    assert_eq!(events, 5, "the five complete lines all arrived");
    assert_eq!(resets, 1, "the truncated line became a Reset marker");
    assert!(disconnected);
    drop(client);
}

#[test]
fn oversize_line_is_a_protocol_error_with_bounded_memory() {
    let path = scripted_server("oversize", |stream| {
        handshake(stream, 1);
        let chunk = vec![b'a'; ipc::MAX_LINE_BYTES + 256 * 1024];
        let _ = stream.write_all(&chunk);
        let _ = stream.write_all(b"\n");
        let _ = stream.write_all(event_line(1).as_bytes());
        let _ = stream.flush();
        std::thread::sleep(Duration::from_secs(1));
    });
    wait_bound(&path, Duration::from_secs(5));
    let (mut client, hello) = UiClient::connect(&path).expect("connect");
    let cursor = hello["initial_snapshot_cursor"].clone();
    let (receiver, _) = client.subscribe_all(&cursor).expect("subscribe");
    let (frames, disconnected) =
        drain_until(&receiver, Duration::from_secs(3));
    let (events, resets) = counts(&frames);
    assert_eq!(events, 0, "no payload past a protocol error");
    assert_eq!(resets, 1, "explicit marker — never silence");
    assert!(disconnected);
    drop(client);
}

#[test]
fn coalesced_ack_and_events_survive_one_write() {
    let path = scripted_server("coalesce", |stream| {
        handshake(stream, 1);
        // ONE write carrying the subscribe ack AND three events: the
        // single persistent reader must keep all of them
        let mut blob =
            String::from(concat!(r#"{"ok":true,"v":1,"subscribed":"*"}"#, "\n"));
        for i in 1..=3usize {
            blob.push_str(&event_line(i));
        }
        let _ = stream.write_all(blob.as_bytes());
        let _ = stream.flush();
        std::thread::sleep(Duration::from_secs(1));
    });
    wait_bound(&path, Duration::from_secs(5));
    let (mut client, hello) = UiClient::connect(&path).expect("connect");
    let cursor = hello["initial_snapshot_cursor"].clone();
    let (receiver, ack) = client.subscribe_all(&cursor).expect("subscribe");
    assert_eq!(ack["ok"], serde_json::Value::Bool(true));
    let (frames, _) = drain_until(&receiver, Duration::from_secs(1));
    let (events, _) = counts(&frames);
    // coalesced survival is about the events; the reader's EOF marker
    // may legitimately appear once the scripted server lets go
    assert_eq!(events, 3, "coalesced events were NOT lost");
    drop(client);
}

#[test]
fn version_mismatch_hello_refused_envelope_refused_and_stale() {
    let path = scripted_server("version", |stream| {
        handshake(stream, 2); // hello says v=2, ok=true
        let mut blob =
            String::from(concat!(r#"{"ok":true,"v":2,"subscribed":"*"}"#, "\n"));
        blob.push_str(
            "{\"type\":\"event\",\"event\":{\"v\":2,\
             \"event_id\":\"x/1/acc\",\"stream_id\":\"x\",\"seq\":1,\
             \"kind\":\"task.accepted\",\"task_id\":\"x\"}}\n",
        );
        let _ = stream.write_all(blob.as_bytes());
        let _ = stream.flush();
        std::thread::sleep(Duration::from_millis(200));
    });
    wait_bound(&path, Duration::from_secs(5));

    // app-level: the wire answers, but application is REFUSED with an
    // explicit error and a stale transition (no baseline from v=2)
    let mut app = SupervisorApp::default();
    let error = app.connect(&path).expect_err("v2 must be refused");
    assert!(
        error.contains("protocol version"),
        "error must name the version drift: {error}"
    );
    assert_eq!(app.reducer.projection.health, Some(Health::Stale));

    // raw client + reducer: connect still works (probes rely on that),
    // but the reducer refuses the v=2 envelope and goes stale
    let (mut client, hello) = UiClient::connect(&path).expect("connect");
    assert!(client.protocol_version_mismatch);
    assert_eq!(hello["v"], serde_json::Value::from(2));
    let cursor = hello["initial_snapshot_cursor"].clone();
    let (receiver, _) = client.subscribe_all(&cursor).expect("subscribe");
    let (frames, _) = drain_until(&receiver, Duration::from_secs(2));
    let mut reducer = Reducer::new();
    reducer.mark_live();
    for frame in frames {
        if let Frame::Event(event) = frame {
            reducer.apply_event(&event);
        }
    }
    assert!(!reducer.projection.tasks.contains_key("x"));
    assert_eq!(reducer.projection.health, Some(Health::Stale));
    drop(client);
}

#[test]
fn disconnect_to_stale_to_bounded_reconnect() {
    // connection 1: handshake, two tasks, then the socket dies;
    // connection 2: handshake again, one task, stays alive
    // connection 2: handshake again, one task, stays alive
    let path = socket_path("reconnect");
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).expect("bind");
    std::thread::spawn(move || {
        let (mut first, _) = listener.accept().expect("accept 1");
        handshake(&mut first, 1);
        let blob = concat!(
            "{\"type\":\"event\",\"event\":{\"v\":1,",
            "\"event_id\":\"a/1/acc\",\"stream_id\":\"a\",\"seq\":1,",
            "\"kind\":\"task.accepted\",\"task_id\":\"a\"}}\n",
            "{\"type\":\"event\",\"event\":{\"v\":1,",
            "\"event_id\":\"b/1/acc\",\"stream_id\":\"b\",\"seq\":1,",
            "\"kind\":\"task.accepted\",\"task_id\":\"b\"}}\n",
        );
        let _ = first.write_all(blob.as_bytes());
        let _ = first.flush();
        std::thread::sleep(Duration::from_millis(100));
        drop(first); // server vanishes

        let (mut second, _) = listener.accept().expect("accept 2");
        handshake(&mut second, 1);
        let blob = concat!(
            "{\"type\":\"event\",\"event\":{\"v\":1,",
            "\"event_id\":\"c/1/acc\",\"stream_id\":\"c\",\"seq\":1,",
            "\"kind\":\"task.accepted\",\"task_id\":\"c\"}}\n",
        );
        let _ = second.write_all(blob.as_bytes());
        let _ = second.flush();
        std::thread::sleep(Duration::from_secs(3));
    });
    wait_bound(&path, Duration::from_secs(5));

    let mut app = SupervisorApp::default();
    app.connect(&path).expect("first connect");
    assert_eq!(app.reducer.projection.health, Some(Health::Live));

    // pump until the disconnect is fully OBSERVED: the Reset frame
    // arrives first (stale + discard), the channel Disconnected lands
    // on a subsequent pass and schedules the bounded reconnect
    let deadline = Instant::now() + Duration::from_secs(3);
    while app.reconnect_at.is_none() {
        app.pump();
        assert!(
            Instant::now() < deadline,
            "disconnect never detected (health={:?}, tasks={})",
            app.reducer.projection.health,
            app.reducer.projection.tasks.len()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(app.reducer.projection.health, Some(Health::Stale));
    assert!(app.reducer.projection.tasks.is_empty());

    // bounded re-connect: clock injected, the backoff honoured
    let due = app.reconnect_at.unwrap();
    std::thread::sleep(Duration::from_millis(50)); // server accepts round 2
    app.pump_with_clock(due + Duration::from_millis(1));
    assert_eq!(app.reducer.projection.health, Some(Health::Live));
    assert_eq!(app.reconnect_attempts, 0);

    // the fresh baseline's task arrives through the new subscription
    let deadline = Instant::now() + Duration::from_secs(3);
    while !app.reducer.knows_task("c") {
        app.pump();
        assert!(Instant::now() < deadline, "task c never re-baselined");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!app.reducer.knows_task("a"), "stale task stayed discarded");
}

// ------------------------------------------------- polish1 request deadline
//
// P3.3 N2: a server that ACCEPTS and then never answers must fail the
// connection at the request deadline instead of blocking forever (the
// RED demo captured pre-fix: connect sat out the server's whole 4 s
// silence window and only the server's EOF unblocked it). The deadline
// covers the REQUEST-RESPONSE phase only (hello / snapshot /
// snapshot-events / subscribe): protocol v1 has NO heartbeat, so a
// quiet EVENT STREAM after the handshake is legal and carries no
// deadline (honest boundary — a v2 heartbeat is separate work).

/// Client level: hello silence → bounded TransportError naming the
/// deadline and the op. The injected 250 ms budget stands in for the
/// 10 s production constant (never sleep real 10 s in tests).
#[test]
fn hello_silence_hits_request_deadline_not_forever() {
    let path = scripted_server("silent-hello", |stream| {
        let _ = read_line(stream); // swallow the hello request…
        std::thread::sleep(Duration::from_secs(5)); // …socket open, silent
    });
    wait_bound(&path, Duration::from_secs(5));
    let start = Instant::now();
    let error = UiClient::connect_with_request_deadline(
        &path,
        Duration::from_millis(250),
    )
    .expect_err("the request deadline must fire");
    assert!(error.0.contains("deadline"), "{}", error.0);
    assert!(error.0.contains("hello"), "{}", error.0);
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "bounded return, not a block: {:?}",
        start.elapsed()
    );
}

/// App level: a hub that answers hello then goes silent on snapshot
/// (the lying-server shape) used to wedge `reconnect_round` forever —
/// including app CREATION. With the deadline the round fails bounded
/// and the app lands in the waiting posture with a bounded rescan:
/// startup can never stay stuck, auto-attach keeps retrying.
#[test]
fn snapshot_silence_deadline_returns_app_to_waiting() {
    let path = scripted_server("silent-snapshot", |stream| {
        let _ = read_line(stream); // hello request
        send(stream, HELLO);
        let _ = read_line(stream); // snapshot request swallowed…
        std::thread::sleep(Duration::from_secs(5)); // …silent, open
    });
    wait_bound(&path, Duration::from_secs(5));

    let mut app = SupervisorApp::default();
    app.connect_deadline = Duration::from_millis(300);
    let entry = app::discovery::HubEntry {
        socket: path.clone(),
        pid: 1,
        project_root: "/proj/silent".into(),
        started_at: 9.0,
    };
    app.discover_fn = Some(Box::new(move || {
        app::discovery::DiscoveryOutcome::Registry(vec![entry.clone()])
    }));
    let start = Instant::now();
    app.reconnect_round(Instant::now()); // must RETURN, bounded
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "round returned bounded: {:?}",
        start.elapsed()
    );
    assert!(app.waiting, "failed attach → waiting posture");
    assert_eq!(app.reducer.projection.health, Some(Health::Stale));
    assert!(app.status_line.contains("deadline"), "{}", app.status_line);
    assert!(app.status_line.contains("waiting"), "{}", app.status_line);
    let wait = app
        .reconnect_at
        .expect("bounded rescan scheduled")
        .saturating_duration_since(Instant::now());
    assert!(wait <= Duration::from_secs(4), "rescan bounded: {wait:?}");
}
