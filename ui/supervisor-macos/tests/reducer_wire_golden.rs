//! Wire-golden replay: the exact frames a real P1.1 UiEventServer sent
//! (recorded by tools/gen_wire_golden.py) are fed through the reducer,
//! and the projection is asserted. This is the Python/Rust shared wire
//! contract: the same JSON drives both sides.

use std::fs;

// The binary's modules are not importable from an integration test, so
// the reducer is re-included via a path include (single source of truth).
#[path = "../src/model.rs"]
mod model;

use model::{Health, Reducer};
use serde_json::Value;

fn golden() -> Value {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/data/wire-golden.json"
    );
    serde_json::from_str(&fs::read_to_string(path).expect("golden file"))
        .expect("golden json")
}

#[test]
fn golden_state_pages_build_projection() {
    let g = golden();
    let mut reducer = Reducer::new();
    for page in g["state_pages"].as_array().unwrap() {
        let states =
            page["states"].as_array().cloned().unwrap_or_default();
        reducer.apply_state_pages(&states);
    }
    // 4 baseline tasks: task-base, task-gap, task-live, task-other
    assert_eq!(reducer.projection.tasks.len(), 4);
    assert!(reducer.knows_task("task-base"));
    assert!(reducer.knows_task("task-gap"));
    assert!(
        reducer.projection.tasks["task-base"].from_state_page
    );
}

#[test]
fn golden_event_pages_fill_log_without_rollback() {
    let g = golden();
    let mut reducer = Reducer::new();
    for page in g["event_pages"].as_array().unwrap() {
        let events =
            page["events"].as_array().cloned().unwrap_or_default();
        reducer.apply_event_pages(&events);
    }
    // 6 envelopes: announcement + accepted pairs; announcement and
    // accepted collapse to ONE task (no double count)
    assert!(reducer.knows_task("task-base"));
    assert_eq!(
        reducer.projection.tasks.len(),
        4,
        "announced + accepted must collapse"
    );
    // the event pages genuinely FILL the projection (not a no-op under
    // the seq guard): task-base's accepted + turn.started replay set
    // worker, session and turn fields (repair1 honest-fill assertion)
    let base = &reducer.projection.tasks["task-base"];
    assert_eq!(base.worker.as_deref(), Some("codex"));
    assert_eq!(base.session_id.as_deref(), Some("sess-task-base"));
    assert_eq!(base.turn_id.as_deref(), Some("turn-task-base"));
    assert_eq!(base.turn_status.as_deref(), Some("running"));
}

#[test]
fn golden_live_stream_discovers_then_reset_discards() {
    let g = golden();
    let mut reducer = Reducer::new();
    for page in g["state_pages"].as_array().unwrap() {
        reducer.apply_state_pages(
            &page["states"].as_array().cloned().unwrap_or_default(),
        );
    }
    // phase 1 — everything EXCEPT the trailing reset_required: the
    // post-cut stream is discovered from seq 1 (no re-hello, no
    // per-task subscribe, no terminal-index read, no manual task id)
    let live = g["live"].as_array().unwrap();
    let (flow, tail): (Vec<_>, Vec<_>) = live
        .iter()
        .partition(|f| f["type"].as_str() != Some("reset_required"));
    for frame in &flow {
        match frame["type"].as_str().unwrap_or("") {
            "event" => reducer.apply_event(&frame["event"]),
            "gap" => reducer.apply_gap(&frame["gap"]),
            _ => {}
        }
    }
    assert!(reducer.knows_task("task-discovered"));
    // ui-polish1: the golden's own wire data pins the accepted display
    // word — after ONLY `task.accepted` (the first live frame) the
    // task's state is its own `accepted`, and the fixture's later
    // `turn.started` promotes it to `running`. The old assertion only
    // saw the collapsed final word and masked the distinction.
    {
        let mut fresh = Reducer::new();
        fresh.apply_event(&flow[0]["event"]); // task.accepted, seq 1
        assert_eq!(
            fresh.projection.tasks["task-discovered"].status,
            "accepted",
            "wire: bare task.accepted must display as accepted"
        );
    }
    assert_eq!(
        reducer.projection.tasks["task-discovered"].status,
        "running",
        "wire: turn.started (seq 2) promoted accepted → running"
    );
    // phase 2 — the oversize-loss reset_required: the projection is
    // DISCARDED (PLAN §4.5.9) and the loss is honestly stale
    for frame in &tail {
        if frame["type"].as_str() == Some("reset_required") {
            reducer.apply_reset();
        }
    }
    assert_eq!(reducer.projection.health, Some(Health::Stale));
    assert!(reducer.projection.tasks.is_empty());
    assert!(reducer.projection.order.is_empty());
    // four-zero counters: standard startup used zero per-task subscribes
    assert_eq!(reducer.per_task_subscribe_count, 0);
    assert_eq!(reducer.terminal_index_reads, 0);
    assert_eq!(reducer.manual_task_id_inputs, 0);
}

#[test]
fn golden_shapes_stable_for_cross_language_diff() {
    // Structural assertions: the golden's keys are exactly the contract
    // the Python side generates; any drift breaks this test loudly.
    let g = golden();
    for key in ["hello", "state_pages", "event_pages", "live"] {
        assert!(g.get(key).is_some(), "golden missing {key}");
    }
    assert_eq!(g["hello"]["ok"], Value::Bool(true));
    for page in g["state_pages"].as_array().unwrap() {
        assert_eq!(page["ok"], Value::Bool(true));
        assert!(page.get("done").is_some());
    }
}
