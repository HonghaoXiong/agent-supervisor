//! The UI reducer: state pages → event pages → live deltas (P1.2).
//!
//! Pure, testable without any GUI: [`Reducer::apply`] ingests whole pages
//! and subscribe frames, and [`Reducer::snapshot`] produces the current
//! task projection. Contract highlights:
//!
//! * state pages build the cut's task projection; event pages replay
//!   fill the log; live deltas with `seq <=` the cut high-water never
//!   roll a state back (later authoritative facts win);
//! * dedup by `event_id` (replay/live overlap is legal, double counting
//!   is not); `task.accepted` + `stream.announced` collapse to ONE task;
//! * streams missing from the cut map arrive from seq 1 (discovery);
//! * `gap` marks ranges unknown (never rendered as complete history),
//!   `reset_required` / version mismatch / disconnect drop the whole
//!   projection and flag `stale` until a fresh baseline is fetched;
//! * `initiator_unverified` never carries the raw strings, and parent
//!   edges are only created for verified records (P0.4 rules).

use std::collections::{HashMap, HashSet};

use serde_json::Value;

/// One task's projected state (the reducer's atomic unit).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TaskState {
    pub task_id: String,
    pub worker: Option<String>,
    pub mode: Option<String>,
    /// running | completed | failed | ... (terminal from task.completed)
    pub status: String,
    pub outcome: Option<String>,
    pub delivery_ok: Option<bool>,
    pub session_id: Option<String>,
    pub turn_id: Option<String>,
    pub turn_status: Option<String>,
    /// `true` when the projection came only from the cut's state pages
    /// and no live/terminal update has been seen since.
    pub from_state_page: bool,
}

/// A gap range marker for one stream.
#[derive(Debug, Clone, PartialEq)]
pub struct GapMarker {
    pub stream_id: String,
    pub from_seq: i64,
    pub to_seq: i64,
    pub count: i64,
}

/// Stored gap markers are bounded: the vec never grows without limit;
/// beyond this many, the OLDEST markers are dropped and counted in
/// `Projection::dropped_gap_markers` (still surfaced in the banners).
pub const MAX_GAP_MARKERS: usize = 1024;

/// Envelope protocol version this reducer speaks (wire contract).
pub const ENVELOPE_VERSION: i64 = 1;

/// Connection/baseline health surfaced in the view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Health {
    /// Baseline + subscription flowing.
    Live,
    /// Reset/version-mismatch/disconnect: projection may be incomplete;
    /// the UI must re-baseline before trusting anything.
    Stale,
}

/// The reducer's output projection.
#[derive(Debug, Clone, Default)]
pub struct Projection {
    pub tasks: HashMap<String, TaskState>,
    /// Ordered task ids as first seen (stable list order).
    pub order: Vec<String>,
    pub gaps: Vec<GapMarker>,
    /// Gap markers dropped once `gaps` hit [`MAX_GAP_MARKERS`].
    pub dropped_gap_markers: usize,
    pub unattributed_count: usize,
    pub health: Option<Health>,
    /// event_ids already applied (dedup).
    seen_event_ids: HashSet<String>,
    /// task ids announced on the hub stream (announcement dedup).
    announced: HashSet<String>,
    /// highest applied seq per stream (state rollback guard).
    stream_seq: HashMap<String, i64>,
}

impl Projection {
    pub fn new() -> Self {
        Projection::default()
    }
}

/// The reducer. Owns the projection and the counters used by the
/// acceptance probes (hello count, per-task subscribe count, terminal
/// index reads, manual task_id inputs — all asserted to stay ZERO in
/// the standard flow).
#[derive(Debug, Default)]
pub struct Reducer {
    pub projection: Projection,
    // -- acceptance counters (four-zero contract) ------------------------
    pub hello_count: usize,
    pub per_task_subscribe_count: usize,
    pub terminal_index_reads: usize,
    pub manual_task_id_inputs: usize,
}

impl Reducer {
    pub fn new() -> Self {
        Reducer::default()
    }

    /// Record that hello happened (once per connection; the counter
    /// exists so tests can assert it stays 1 through all three phases).
    pub fn note_hello(&mut self) {
        self.hello_count += 1;
    }

    /// Apply one batch of state-page records (the cut's task states).
    /// Idempotent per task within the same snapshot_id: the reducer
    /// trusts the server's per-snapshot dedup and overwrites locally.
    pub fn apply_state_pages(&mut self, states: &[Value]) {
        for record in states {
            let task_id = record.get("task_id").and_then(|v| v.as_str());
            let Some(task_id) = task_id else { continue };
            let raw_status = record
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("running");
            // ui-polish1 + repair1 (R1): the server's reduce_stream
            // (ui_journal.py) words an accepted-only stream `running`
            // with session_id / turn_id / outcome all null — that SHAPE
            // alone is NOT sufficient. The journal's global FIFO
            // retention (§6.3) can evict a stream's accepted/bound/
            // turn.started prefix BEFORE the cut; `new_cut` silently
            // rebases above the loss (no reset_required — §6.2's
            // completeness covers only the cut's promised range), and
            // the folded suffix of a GENUINELY RUNNING task carries the
            // exact same shape. The discriminator is
            // `accepted_event_id`: a true accepted-only stream folds
            // its `task.accepted` event into a non-null value; an
            // eviction-rebased record never saw the event (null). The
            // display word `accepted` derives ONLY when the shape AND
            // the discriminator agree; ambiguous records fall back to
            // the server word (`running`) — never a wrong `accepted`.
            let has_accepted_event_id = record
                .get("accepted_event_id")
                .and_then(|v| v.as_str())
                .is_some();
            let status = if accepted_only_state_page(
                raw_status,
                str_field(record, "session_id").is_none()
                    && str_field(record, "turn_id").is_none()
                    && str_field(record, "outcome").is_none(),
                has_accepted_event_id,
            ) {
                "accepted"
            } else {
                raw_status
            }
            .to_string();
            let entry = self
                .projection
                .tasks
                .entry(task_id.to_string())
                .or_insert_with(|| {
                    self.projection.order.push(task_id.to_string());
                    TaskState {
                        task_id: task_id.to_string(),
                        ..Default::default()
                    }
                });
            // within a state page the record IS the projection; a later
            // live terminal (or a later page of the same snapshot)
            // overwrites — but a stale page never un-terminals a task.
            // `cancelled` IS terminal (rollback guard hole fixed in
            // repair1: the model set and the view set now agree).
            let incoming_terminal = matches!(
                status.as_str(),
                "completed" | "failed" | "timeout" | "cancelled"
            );
            let current_terminal = matches!(
                entry.status.as_str(),
                "completed" | "failed" | "timeout" | "cancelled"
            );
            if !current_terminal || incoming_terminal {
                entry.worker = str_field(record, "worker");
                entry.mode = str_field(record, "mode");
                entry.status = status;
                entry.outcome = str_field(record, "outcome");
                entry.delivery_ok =
                    record.get("delivery_ok").and_then(|v| v.as_bool());
                entry.session_id = str_field(record, "session_id");
                entry.turn_id = str_field(record, "turn_id");
                entry.turn_status = str_field(record, "turn_status");
            }
            entry.from_state_page = true;
            if let Some(seq) = record.get("last_seq").and_then(|v| v.as_i64())
            {
                let top = self
                    .projection
                    .stream_seq
                    .entry(task_id.to_string())
                    .or_insert(0);
                if seq > *top {
                    *top = seq;
                }
            }
        }
    }

    /// Apply one batch of event-page envelopes (the cut's event replay).
    /// Fills the log view; states never roll back (seq guard).
    pub fn apply_event_pages(&mut self, events: &[Value]) {
        for event in events {
            self.apply_event(event);
        }
    }

    /// Apply one live subscribe frame's event envelope (also used by
    /// event pages). Version gate first: `v != 1` is refused AND flags
    /// stale (repair1 #4) — a version-drifted server may not mutate the
    /// projection. Then dedup by event_id; state rollback guard per
    /// stream.
    pub fn apply_event(&mut self, event: &Value) {
        if event.get("v").and_then(|v| v.as_i64())
            != Some(ENVELOPE_VERSION)
        {
            // protocol drift: refuse to apply, surface stale
            self.projection.health = Some(Health::Stale);
            return;
        }
        let Some(event_id) = event.get("event_id").and_then(|v| v.as_str())
        else {
            // unattributable envelope: counted, never forced onto a task
            self.projection.unattributed_count += 1;
            return;
        };
        if !self.projection.seen_event_ids.insert(event_id.to_string()) {
            return; // duplicate event_id: replay/live overlap — dropped
        }
        let stream_id = event
            .get("stream_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let seq = event.get("seq").and_then(|v| v.as_i64()).unwrap_or(0);
        let top = self
            .projection
            .stream_seq
            .entry(stream_id.to_string())
            .or_insert(0);
        if seq <= *top {
            return; // out-of-order or pre-cut replay of another stream's
                    // frame: states never roll back
        }
        *top = seq;

        let kind = event.get("kind").and_then(|v| v.as_str()).unwrap_or("");
        match kind {
            "task.accepted" => {
                let entry = self
                    .projection
                    .tasks
                    .entry(stream_id.to_string())
                    .or_insert_with(|| {
                        self.projection.order.push(stream_id.to_string());
                        TaskState {
                            task_id: stream_id.to_string(),
                            ..Default::default()
                        }
                    });
                if entry.status.is_empty() {
                    // ui-polish1: accepted is a first-class display
                    // state (UI-EVENTS-V1 §5.2); the word stays until
                    // the first session/turn activity promotes it
                    entry.status = "accepted".into();
                }
                entry.worker = str_field(event, "worker");
                entry.mode = str_field(event, "mode");
                entry.from_state_page = false;
            }
            "stream.announced" => {
                // announcement + accepted collapse to ONE task; no double
                // count, no separate row
                if let Some(announced_id) = event
                    .get("data")
                    .and_then(|d| d.get("task_stream_id"))
                    .and_then(|v| v.as_str())
                {
                    self.projection.announced.insert(announced_id.to_string());
                }
            }
            "session.bound" | "turn.started" | "turn.completed" => {
                if let Some(entry) = self.projection.tasks.get_mut(stream_id)
                {
                    if let Some(session) = str_field(event, "session_id") {
                        entry.session_id = Some(session);
                    }
                    if let Some(turn) = str_field(event, "turn_id") {
                        entry.turn_id = Some(turn);
                    }
                    if kind == "turn.started" {
                        entry.turn_status = Some("running".into());
                        entry.from_state_page = false;
                    }
                    if kind == "turn.completed" {
                        if let Some(status) = str_field(event, "status") {
                            entry.turn_status = Some(status);
                        }
                    }
                    // ui-polish1: session/turn activity is the first
                    // lifecycle fact past acceptance — `accepted`
                    // promotes to `running` here (never rolls a
                    // terminal back: those were handled above and this
                    // arm only narrows the accepted word).
                    if entry.status == "accepted" {
                        entry.status = "running".into();
                    }
                }
            }
            "task.completed" => {
                if let Some(entry) = self.projection.tasks.get_mut(stream_id)
                {
                    entry.status = event
                        .get("status")
                        .and_then(|v| v.as_str())
                        .unwrap_or("completed")
                        .to_string();
                    entry.outcome = str_field(event, "outcome");
                    entry.delivery_ok =
                        event.get("delivery_ok").and_then(|v| v.as_bool());
                    entry.from_state_page = false;
                }
            }
            _ => {} // tool/approval/process events: log-only here
        }
    }

    /// Apply a gap marker: the range is unknown, never complete
    /// history. The lost seqs will NOT be re-delivered on this
    /// subscription, so the stream's rollback guard advances past the
    /// range — a frame arriving "inside" the gap is dropped by the
    /// normal seq guard while the marker keeps the loss visible
    /// (repair1: documented gap semantics, probe `gap_range_events_
    /// dropped_by_seq_guard`). Stored markers are bounded
    /// ([`MAX_GAP_MARKERS`]); overflow drops the OLDEST and counts it.
    pub fn apply_gap(&mut self, gap: &Value) {
        let Some(stream_id) = gap.get("stream_id").and_then(|v| v.as_str())
        else {
            return;
        };
        let to_seq = gap.get("to_seq").and_then(|v| v.as_i64()).unwrap_or(0);
        let top = self
            .projection
            .stream_seq
            .entry(stream_id.to_string())
            .or_insert(0);
        if to_seq > *top {
            *top = to_seq;
        }
        self.projection.gaps.push(GapMarker {
            stream_id: stream_id.to_string(),
            from_seq: gap.get("from_seq").and_then(|v| v.as_i64()).unwrap_or(0),
            to_seq,
            count: gap.get("count").and_then(|v| v.as_i64()).unwrap_or(0),
        });
        if self.projection.gaps.len() > MAX_GAP_MARKERS {
            self.projection.gaps.remove(0);
            self.projection.dropped_gap_markers += 1;
        }
    }

    /// Apply a reset marker / version mismatch / disconnect: the whole
    /// projection is DISCARDED (PLAN §4.5.9 — stale tasks must not
    /// survive a re-baseline) and health goes stale until a fresh
    /// hello → pages baseline calls [`Reducer::mark_live`].
    pub fn apply_reset(&mut self) {
        self.projection.tasks.clear();
        self.projection.order.clear();
        self.projection.gaps.clear();
        self.projection.unattributed_count = 0;
        self.projection.seen_event_ids.clear();
        self.projection.announced.clear();
        self.projection.stream_seq.clear();
        self.projection.health = Some(Health::Stale);
    }

    /// A fresh baseline (post-reset re-hello + snapshot) clears stale.
    pub fn mark_live(&mut self) {
        self.projection.health = Some(Health::Live);
    }

    /// True when a task exists by accepted OR announcement (dedup view).
    pub fn knows_task(&self, task_id: &str) -> bool {
        self.projection.tasks.contains_key(task_id)
            || self.projection.announced.contains(task_id)
    }

    /// Present-probe support: true when an envelope with this
    /// `event_id` has passed the version gate and the dedup set once
    /// in the current baseline (i.e. the reducer ingested it — its
    /// state may feed frames). Boundary: an envelope dropped by the
    /// per-stream seq guard (out-of-order / inside a gap) still counts
    /// as seen, so a paint mark for such an id attests ingestion, not
    /// a projection change.
    pub fn saw_event(&self, event_id: &str) -> bool {
        self.projection.seen_event_ids.contains(event_id)
    }
}

fn str_field(record: &Value, key: &str) -> Option<String> {
    record
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// A state-page record is "accepted-only" when the server word is
/// `running`, no session/turn/outcome fact exists, AND the record
/// actually folded its `task.accepted` event (`accepted_event_id`
/// non-null). The last clause is the R1 discriminator: without it, a
/// stream whose accepted/bound/turn.started prefix was evicted by the
/// global FIFO retention (§6.3) before the cut — silently rebased by
/// `new_cut`, no `reset_required` — folds its retained suffix into the
/// same shape while the task is GENUINELY RUNNING. Ambiguous records
/// must fall back to the server word, never derive `accepted`. Pure
/// function so the mapping is unit-testable headless (ui-polish1,
/// repair1 R1).
fn accepted_only_state_page(
    raw_status: &str,
    no_activity_facts: bool,
    has_accepted_event_id: bool,
) -> bool {
    raw_status == "running" && no_activity_facts && has_accepted_event_id
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(stream: &str, seq: i64, kind: &str) -> Value {
        json!({
            "v": 1, "event_id": format!("{stream}/{seq}/{kind}"),
            "stream_id": stream, "seq": seq, "kind": kind,
            "task_id": stream, "worker": "codex", "mode": "app-server",
            "session_id": "s", "turn_id": "t",
        })
    }

    #[test]
    fn state_pages_then_events_no_rollback() {
        let mut reducer = Reducer::new();
        reducer.apply_state_pages(&[json!({
            "task_id": "a", "worker": "codex", "mode": "m",
            "status": "running", "last_seq": 2,
        })]);
        reducer.apply_event_pages(&[event("a", 3, "task.completed")]);
        let task = reducer.projection.tasks.get("a").unwrap();
        assert_eq!(task.status, "completed");
        // a late state page replay never un-terminals it
        reducer.apply_state_pages(&[json!({
            "task_id": "a", "status": "running", "last_seq": 2,
        })]);
        assert_eq!(reducer.projection.tasks.get("a").unwrap().status, "completed");
    }

    #[test]
    fn duplicate_event_id_counted_once() {
        let mut reducer = Reducer::new();
        let e = event("a", 1, "task.accepted");
        reducer.apply_event(&e);
        reducer.apply_event(&e);
        assert!(reducer.projection.tasks.contains_key("a"));
        assert_eq!(reducer.projection.tasks.len(), 1);
    }

    #[test]
    fn accepted_and_announcement_one_task() {
        let mut reducer = Reducer::new();
        reducer.apply_event(&event("a", 1, "task.accepted"));
        reducer.apply_event(&json!({
            "event_id": "hub/2/ann", "stream_id": "hub", "seq": 2,
            "kind": "stream.announced", "task_id": null,
            "data": {"task_stream_id": "a", "accepted_event_id": "x"},
        }));
        assert!(reducer.knows_task("a"));
        assert_eq!(reducer.projection.tasks.len(), 1);
    }

    #[test]
    fn new_stream_seq1_discovery() {
        let mut reducer = Reducer::new();
        reducer.apply_event(&event("new", 1, "task.accepted"));
        assert!(reducer.projection.tasks.contains_key("new"));
    }

    #[test]
    fn out_of_order_other_stream_frame_ignored_for_state() {
        let mut reducer = Reducer::new();
        reducer.apply_event(&event("a", 5, "task.accepted"));
        reducer.apply_event(&event("a", 2, "task.accepted"));
        let top = reducer.projection.stream_seq.get("a").copied();
        assert_eq!(top, Some(5));
    }

    #[test]
    fn gap_marked_then_reset_discards_everything() {
        let mut reducer = Reducer::new();
        reducer.apply_gap(&json!({
            "stream_id": "a", "from_seq": 2, "to_seq": 2, "count": 1}));
        assert_eq!(reducer.projection.gaps.len(), 1);
        reducer.apply_reset();
        // repair1: reset DISCARDS the projection — gap markers too
        assert_eq!(reducer.projection.gaps.len(), 0);
        assert_eq!(reducer.projection.health, Some(Health::Stale));
        reducer.mark_live();
        assert_eq!(reducer.projection.health, Some(Health::Live));
    }

    #[test]
    fn four_zero_counters_stay_zero_by_default() {
        let reducer = Reducer::new();
        assert_eq!(reducer.per_task_subscribe_count, 0);
        assert_eq!(reducer.terminal_index_reads, 0);
        assert_eq!(reducer.manual_task_id_inputs, 0);
    }

    // -- repair1 semantics -------------------------------------------------

    #[test]
    fn cancelled_is_terminal_no_stale_page_rollback() {
        let mut reducer = Reducer::new();
        reducer.apply_event(&json!({
            "v": 1, "event_id": "a/1/acc", "stream_id": "a", "seq": 1,
            "kind": "task.accepted", "task_id": "a",
        }));
        reducer.apply_event(&json!({
            "v": 1, "event_id": "a/2/done", "stream_id": "a", "seq": 2,
            "kind": "task.completed", "task_id": "a", "status": "cancelled",
        }));
        // a STALE state page (last_seq 1 < the terminal's seq 2) must
        // not un-terminal a cancelled task
        reducer.apply_state_pages(&[json!({
            "task_id": "a", "status": "running", "last_seq": 1,
        })]);
        assert_eq!(
            reducer.projection.tasks["a"].status,
            "cancelled",
            "model terminal set must include cancelled"
        );
    }

    #[test]
    fn reset_discards_the_whole_projection() {
        let mut reducer = Reducer::new();
        reducer.apply_event(&event("a", 1, "task.accepted"));
        reducer.apply_event(&event("b", 1, "task.accepted"));
        reducer.mark_live();
        reducer.apply_reset();
        // PLAN §4.5.9: discarded — nothing survives into the next
        // baseline, stale or otherwise
        assert!(reducer.projection.tasks.is_empty());
        assert!(reducer.projection.order.is_empty());
        assert!(reducer.projection.seen_event_ids.is_empty());
        assert_eq!(reducer.projection.health, Some(Health::Stale));
        // fresh baseline carries only what the new snapshot says
        reducer.apply_state_pages(&[json!({
            "task_id": "a", "status": "running", "last_seq": 1,
        })]);
        assert!(reducer.projection.tasks.contains_key("a"));
        assert!(!reducer.projection.tasks.contains_key("b"));
    }

    #[test]
    fn envelope_version_mismatch_refused_and_stale() {
        let mut reducer = Reducer::new();
        reducer.mark_live();
        let mut drifted = event("a", 1, "task.accepted");
        drifted["v"] = json!(2);
        reducer.apply_event(&drifted);
        // refused: no task row, and the drift is OBSERVABLE as stale
        assert!(!reducer.projection.tasks.contains_key("a"));
        assert_eq!(reducer.projection.health, Some(Health::Stale));
        // the SAME event_id at v=1 is still applyable (the refusal did
        // not poison the dedup set)
        reducer.apply_reset(); // clears stale; dedup set is empty again
        reducer.apply_event(&event("a", 1, "task.accepted"));
        assert!(reducer.projection.tasks.contains_key("a"));
    }

    #[test]
    fn gap_advances_seq_guard_and_marker_persists() {
        let mut reducer = Reducer::new();
        reducer.apply_event(&event("a", 1, "task.accepted"));
        reducer.apply_gap(&json!({
            "stream_id": "a", "from_seq": 2, "to_seq": 4, "count": 3}));
        // a terminal arriving INSIDE the gapped range (seq 3 <= top 4)
        // is dropped by the seq guard; the gap marker remains — the
        // loss stays visible, no phantom rollback
        reducer.apply_event(&json!({
            "v": 1, "event_id": "a/3/done", "stream_id": "a", "seq": 3,
            "kind": "task.completed", "task_id": "a", "status": "completed",
        }));
        // ui-polish1: the bare accepted event leaves the task in its
        // OWN display state — `accepted`, NOT `running` (the old
        // assertion pinned the collapsed mapping the live session saw)
        assert_eq!(reducer.projection.tasks["a"].status, "accepted");
        assert_eq!(reducer.projection.gaps.len(), 1);
        // events PAST the gap still flow normally
        reducer.apply_event(&json!({
            "v": 1, "event_id": "a/5/done", "stream_id": "a", "seq": 5,
            "kind": "task.completed", "task_id": "a", "status": "completed",
        }));
        assert_eq!(reducer.projection.tasks["a"].status, "completed");
    }

    #[test]
    fn gap_markers_are_bounded_with_counter() {
        let mut reducer = Reducer::new();
        for i in 0..(MAX_GAP_MARKERS + 5) {
            reducer.apply_gap(&json!({
                "stream_id": "s", "from_seq": i as i64,
                "to_seq": i as i64, "count": 1}));
        }
        assert_eq!(reducer.projection.gaps.len(), MAX_GAP_MARKERS);
        assert_eq!(reducer.projection.dropped_gap_markers, 5);
    }

    // -- ui-polish1: `accepted` is a first-class display state -------------
    //
    // UI-EVENTS-V1 §5.2 lists `task.accepted` as its own lifecycle kind
    // and §5.4 orders it BEFORE any session/turn activity. The reducer
    // therefore keeps an accepted-only task in its own state word until
    // the first activity fact arrives; it never collapses it to
    // `running` (the live-session defect: accepted rows displayed
    // "running").

    #[test]
    fn accepted_is_distinct_display_state() {
        let mut reducer = Reducer::new();
        reducer.apply_event(&event("a", 1, "task.accepted"));
        assert_eq!(
            reducer.projection.tasks["a"].status,
            "accepted",
            "bare task.accepted must display as its own state, not running"
        );
        // task.completed still wins (later authoritative fact)
        reducer.apply_event(&json!({
            "v": 1, "event_id": "a/2/done", "stream_id": "a", "seq": 2,
            "kind": "task.completed", "task_id": "a", "status": "completed",
        }));
        assert_eq!(reducer.projection.tasks["a"].status, "completed");
    }

    #[test]
    fn accepted_promotes_to_running_on_first_activity() {
        for kind in ["session.bound", "turn.started"] {
            let mut reducer = Reducer::new();
            reducer.apply_event(&event("a", 1, "task.accepted"));
            reducer.apply_event(&event("a", 2, kind));
            assert_eq!(
                reducer.projection.tasks["a"].status,
                "running",
                "{kind} is the first activity fact — accepted promotes"
            );
        }
        // without activity the word stays
        let mut reducer = Reducer::new();
        reducer.apply_event(&event("b", 1, "task.accepted"));
        assert_eq!(reducer.projection.tasks["b"].status, "accepted");
    }

    #[test]
    fn state_page_accepted_only_shape_maps_to_accepted() {
        // The server's reduce_stream (ui_journal.py) words an accepted-only
        // stream `running` with session_id/turn_id/outcome all null AND
        // `accepted_event_id` set (it folded the task.accepted event) —
        // only THEN does the UI derive the display word `accepted`
        // (repair1 R1: the shape alone is ambiguous under §6.3 retention
        // eviction, see the fixture-driven regression in
        // tests/eviction_rebased_state.rs).
        let mut reducer = Reducer::new();
        reducer.apply_state_pages(&[json!({
            "task_id": "echo", "worker": "codex", "status": "running",
            "session_id": null, "turn_id": null, "outcome": null,
            "accepted_event_id": "h/1/echo/1", "last_seq": 1,
        })]);
        assert_eq!(reducer.projection.tasks["echo"].status, "accepted");
        // eviction-rebased / pre-discriminator records (accepted_event_id
        // absent or null) keep the server word: a genuinely running task
        // must never display `accepted` (R1)
        reducer.apply_state_pages(&[json!({
            "task_id": "hotel", "worker": "codex", "status": "running",
            "session_id": null, "turn_id": null, "outcome": null,
            "accepted_event_id": null, "last_seq": 5,
        })]);
        assert_eq!(reducer.projection.tasks["hotel"].status, "running");
        // a session-bearing record is genuinely running
        reducer.apply_state_pages(&[json!({
            "task_id": "bravo", "worker": "codex", "status": "running",
            "session_id": "s1", "turn_id": "t1",
            "accepted_event_id": "h/1/bravo/1", "last_seq": 3,
        })]);
        assert_eq!(reducer.projection.tasks["bravo"].status, "running");
        // a LATER state page with activity promotes an accepted task
        reducer.apply_state_pages(&[json!({
            "task_id": "echo", "worker": "codex", "status": "running",
            "session_id": "s2", "turn_id": "t2",
            "accepted_event_id": "h/1/echo/1", "last_seq": 4,
        })]);
        assert_eq!(reducer.projection.tasks["echo"].status, "running");
    }
}
