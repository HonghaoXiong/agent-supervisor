//! R1 regression (ui-polish1 repair1): the accepted state-page SHAPE is
//! ambiguous on its own — a stream whose accepted/session/turn prefix
//! was evicted by the journal's global FIFO retention (§6.3) BEFORE the
//! cut is silently rebased by `new_cut` (no `reset_required`; §6.2's
//! completeness only covers the cut's own promised range), and
//! `reduce_stream` then folds the retained suffix into
//! `{status: running, session/turn/outcome: null}` for a task that is
//! GENUINELY RUNNING.
//!
//! The discriminating field is `accepted_event_id`: a true accepted-only
//! stream folds its `task.accepted` event into a non-null value, while
//! an eviction-rebased record never saw the event and carries null. The
//! UI reducer may derive the `accepted` display word ONLY when that
//! field is present (reviewer-verified against the production path).
//!
//! The fixture is NOT synthetic: `tests/data/eviction-rebased-state.json`
//! was recorded through the REAL `UiJournal.ingest → global FIFO
//! eviction → new_cut → snapshot → reduce_stream` pipeline (the exact op
//! `ui_server.py` serves), with `retention_floors` proving the prefix
//! loss (`task-evict-running` floor = 4 of last_seq = 5).

#[path = "../src/model.rs"]
mod model;
#[path = "../src/view.rs"]
mod view;

use model::Reducer;
use serde_json::Value;
use std::fs;
use view::{footer_of, Filter};

fn fixture() -> Value {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/data/eviction-rebased-state.json"
    );
    serde_json::from_str(&fs::read_to_string(path).expect("fixture file"))
        .expect("fixture json")
}

#[test]
fn eviction_rebased_running_never_displays_accepted() {
    let f = fixture();
    let mut reducer = Reducer::new();
    reducer.apply_state_pages(f["records"].as_array().unwrap());
    let tasks = &reducer.projection.tasks;

    // fixture sanity: the eviction actually happened (floor 4 < last 5)
    // and the discriminator fields are where the provenance says they
    // are — null on the rebased record, present on the true one
    assert_eq!(
        f["retention_floors"]["task-evict-running"].as_i64(),
        Some(4),
        "fixture drift: the prefix eviction is the point of this test"
    );
    let record_of = |id: &str| {
        f["records"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["task_id"] == Value::String(id.into()))
            .unwrap_or_else(|| panic!("fixture missing {id}"))
            .clone()
    };
    assert!(
        record_of("task-evict-running")["accepted_event_id"].is_null(),
        "fixture drift: rebased record must carry accepted_event_id=null"
    );
    assert!(
        record_of("task-accepted-only")["accepted_event_id"].is_string(),
        "fixture drift: true accepted-only record must carry the id"
    );

    // R1 RED/GREEN core: the eviction-rebased GENUINELY-RUNNING task
    // (open turn-1, tools firing) must display `running`, never
    // `accepted`, even though its record carries the accepted-only shape
    assert_eq!(
        tasks["task-evict-running"].status, "running",
        "eviction-rebased running task misdisplayed as accepted (R1)"
    );
    // the tool-only filler stream is the same ambiguity class
    assert_eq!(tasks["task-filler"].status, "running");
    // the TRUE accepted-only stream (accepted_event_id present) still
    // derives its own display word
    assert_eq!(
        tasks["task-accepted-only"].status, "accepted",
        "the true positive must survive the gate"
    );
    // terminal record unaffected
    assert_eq!(tasks["task-completed"].status, "completed");
}

#[test]
fn footer_counts_follow_the_repaired_derivation() {
    let f = fixture();
    let mut reducer = Reducer::new();
    reducer.apply_state_pages(f["records"].as_array().unwrap());
    let counts = footer_of(&reducer.projection, &Filter::default());
    // 2 running (evicted-rebased + tool-filler), 1 accepted (true),
    // 1 completed — NOT 3 accepted (the pre-repair footer error)
    assert_eq!(counts.running, 2, "counts: {counts:?}");
    assert_eq!(counts.accepted, 1, "counts: {counts:?}");
    assert_eq!(counts.completed, 1, "counts: {counts:?}");
    assert_eq!(counts.failed, 0);
    assert_eq!(counts.other, 0);
}
