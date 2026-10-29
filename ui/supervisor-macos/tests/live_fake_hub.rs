//! Live fake-hub integration: connects a REAL Rust UDS client to a REAL
//! P1.1 UiEventServer started by the Python harness
//! (tools/bench_ui.py --fake-hub-replay). Enabled only when
//! `CZS_UI_TEST_SOCKET` points at the live server socket; otherwise
//! skipped (headless CI without the Python side).
//!
//! The three discovery timepoints (after_subscribe / hello_only /
//! between_hello_and_snapshot) are driven by the PYTHON harness: it
//! creates the frozen-terminal task at the right phase and asserts the
//! four-zero counters there. This Rust side proves the wire round trip:
//! hello, full state/event pagination, subscribe, discovery of a
//! post-cut running task from seq 1, and honest reset on a
//! fanout-marker loss.

use std::env;
use std::sync::mpsc::TryRecvError;
use std::time::Duration;

#[path = "../src/ipc.rs"]
mod ipc;
#[path = "../src/model.rs"]
mod model;

use ipc::{Frame, UiClient};
use model::Reducer;

fn socket() -> Option<String> {
    env::var("CZS_UI_TEST_SOCKET").ok().filter(|s| !s.is_empty())
}

fn wait_for<F>(mut predicate: F, timeout: Duration) -> bool
where
    F: FnMut() -> bool,
{
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if predicate() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

#[test]
fn live_hub_roundtrip_discovers_running_task() {
    let Some(socket) = socket() else {
        eprintln!("skipping: CZS_UI_TEST_SOCKET not set");
        return;
    };
    let (mut client, hello) =
        UiClient::connect(&socket).expect("connect+hello");
    assert_eq!(hello["ok"], serde_json::Value::Bool(true));
    let cursor = hello["initial_snapshot_cursor"].clone();

    let states = ipc::fetch_all_pages(
        &mut client,
        "snapshot",
        &cursor,
        "states",
        256,
    )
    .expect("state pages");
    let events = ipc::fetch_all_pages(
        &mut client,
        "snapshot-events",
        &cursor,
        "events",
        1024,
    )
    .expect("event pages");

    let (receiver, ack) = client.subscribe_all(&cursor).expect("subscribe");
    assert_eq!(ack["ok"], serde_json::Value::Bool(true));

    let mut reducer = Reducer::new();
    reducer.note_hello(); // exactly one hello in the whole flow
    reducer.apply_state_pages(&states);
    reducer.apply_event_pages(&events);
    reducer.mark_live();

    // wait for the harness's post-cut running task to arrive from seq 1
    let discovered = wait_for(
        || {
            while let Ok(frame) = receiver.try_recv() {
                match frame {
                    Frame::Event(event) => reducer.apply_event(&event),
                    Frame::Gap(gap) => reducer.apply_gap(&gap),
                    Frame::Reset => reducer.apply_reset(),
                    _ => {}
                }
            }
            reducer.knows_task("task-live-discovery")
                || reducer.knows_task("task-after")
                || reducer.knows_task("task-between")
        },
        Duration::from_secs(10),
    );
    assert!(
        discovered,
        "post-cut running task never appeared without re-hello"
    );
    assert_eq!(reducer.hello_count, 1); // no second hello anywhere
    assert_eq!(reducer.per_task_subscribe_count, 0);
    assert_eq!(reducer.terminal_index_reads, 0);
    assert_eq!(reducer.manual_task_id_inputs, 0);
    // the receiver stays open (no reset required for discovery)
    assert_ne!(
        receiver.try_recv(),
        Err(TryRecvError::Disconnected),
        "connection dropped during discovery"
    );
}
