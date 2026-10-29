//! Present probe (PLAN §5.3.5–§5.3.6) — a REAL renderer-layer probe,
//! with its semantics bounded honestly.
//!
//! What layer this actually measures, and what it must NOT be called:
//!
//! * PLAN §5.3.6 defines `frame_submitted` as the app-side mark taken
//!   AFTER the frame's draw completed AND was submitted to the window
//!   system. eframe 0.37 exposes NO application-visible callback after
//!   the surface present: the wgpu backend calls
//!   `queue.present(output_frame)` inside `egui-wgpu`'s
//!   `paint_and_update_textures` and the glow backend calls
//!   `gl_surface.swap_buffers` inside eframe's glow integration, both
//!   with no hook after them (verified against the vendored 0.37.0
//!   sources). This probe therefore NEVER emits a `frame_submitted_ns`
//!   key, and end-to-end `source_to_present` stays reportable only as
//!   `unavailable`.
//!
//! * The best REAL layer obtainable is the backend paint callback:
//!   [`egui_wgpu::CallbackTrait::paint`] executes INSIDE the same
//!   `wgpu::RenderPass` that records every egui draw command of the
//!   frame — after tessellation, and strictly before `encoder.finish()`
//!   / `queue.submit()` / the present. The probe registers its callback
//!   as the last shape of the top (`Foreground`) layer, so its
//!   timestamp is taken after all of the frame's other draw commands
//!   were recorded into the pass (command-record order; a tiling GPU
//!   may reorder hardware execution, but record order is the
//!   deterministic egui guarantee). Emitted key: `frame_paint_ns`.
//!   Semantics: "the renderer's render pass for a frame whose reducer
//!   state contains this event_id has reached the probe" — a real
//!   draw-stage mark, NOT a present/compositor mark, and NOT the
//!   app-side widget-build mark (`ui_drawn_ns`).
//!
//! Event scoping: tracked event ids come from `--probe-events` (CLI,
//! comma-separated) or `CZS_UI_PROBE_EVENTS`; the sink is the
//! `CZS_UI_SIDECAR` path (append) or stdout. An id becomes markable
//! only once the reducer has actually ingested it (version gate +
//! dedup set — see [`crate::model::Reducer::saw_event`]); a reducer
//! reset demotes an un-marked id back to waiting, because its state no
//! longer feeds any frame. Each id is marked at most once (first
//! presentation). With no tracked ids the probe is fully inert: no
//! callback shape, no output, no behavioral change — the read-only
//! face is untouched (the callback issues zero draw commands).
//!
//! Clock: `frame_paint_ns` comes from the SAME `monotonic_ns()` as
//! `journal_enqueued_ns` / `ui_drawn_ns` (std `Instant`; on macOS
//! backed by `clock_gettime(CLOCK_UPTIME_RAW)`), so deltas among the
//! app-side marks are exact. Cross-language absolute comparison
//! against Python's `CLOCK_MONOTONIC` still requires the §5.3.5 clock
//! probe (boot-origin vs process-origin offset, sleep drift).

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};

use eframe::egui;

use super::model::Reducer;

/// Sidecar key for this probe's mark. Deliberately NOT
/// `frame_submitted_ns`: this layer is pre-submit/pre-present.
pub const MARK_KEY: &str = "frame_paint_ns";

/// Identifies the probe layer in every sidecar line.
pub const PROBE_LAYER: &str = "wgpu_render_pass";

/// Per-event lifecycle inside the probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Track {
    /// Configured but not yet ingested by the reducer.
    Waiting,
    /// The reducer ingested the event (its projection may draw it);
    /// carries the `journal_enqueued_ns` observed at promotion.
    Applied(Option<u128>),
    /// First paint recorded — terminal, never re-marked.
    Marked,
}

/// Where sidecar lines go.
#[derive(Debug)]
enum Sink {
    Stdout,
    File(std::fs::File),
}

impl Sink {
    fn write_line(&mut self, line: &str) {
        match self {
            Sink::Stdout => println!("{line}"),
            Sink::File(file) => {
                let _ = writeln!(file, "{line}");
                let _ = file.flush();
            }
        }
    }
}

/// Shared, lockable probe state — read by the app thread (`observe`,
/// `paint_probe`) and by the render thread's paint callback
/// (`mark_frame_paint`).
#[derive(Debug)]
struct ProbeInner {
    /// Tracked ids in configured order (deterministic replay).
    order: Vec<String>,
    states: HashMap<String, Track>,
    sink: Sink,
}

impl ProbeInner {
    fn all_marked(&self) -> bool {
        self.states.values().all(|state| *state == Track::Marked)
    }

    /// The paint-callback body (GUI-free, unit-tested): mark every
    /// `Applied` id with `now` and emit one sidecar line per id.
    fn mark_frame_paint(&mut self, now: u128) {
        let newly: Vec<String> = self
            .order
            .iter()
            .filter(|id| matches!(self.states.get(*id), Some(Track::Applied(_))))
            .cloned()
            .collect();
        for id in newly {
            let journal = match self.states.get(&id) {
                Some(Track::Applied(journal)) => *journal,
                _ => continue, // raced to Marked/Waiting: skip
            };
            let line = serde_json::json!({
                "probe": "present",
                "probe_layer": PROBE_LAYER,
                "event_id": id,
                MARK_KEY: ns_to_u64(now),
                "journal_enqueued_ns":
                    journal.map(ns_to_u64),
            })
            .to_string();
            self.sink.write_line(&line);
            self.states.insert(id, Track::Marked);
        }
    }
}

/// u64 view of a monotonic nanosecond stamp (process-origin ns; fits
/// u64 for any plausible uptime — saturate rather than panic in the
/// render thread).
fn ns_to_u64(ns: u128) -> u64 {
    u64::try_from(ns).unwrap_or(u64::MAX)
}

/// The present probe. `None` inside = inactive (default): zero cost,
/// zero output, no callback.
#[derive(Debug, Default)]
pub struct PresentProbe {
    shared: Option<Arc<Mutex<ProbeInner>>>,
}

/// Tracked-id list from CLI args: `--probe-events=a,b` (preferred)
/// or `--probe-events a,b`. Pure — no environment access, so tests can
/// drive it deterministically.
pub fn tracked_from_args(args: &[String]) -> Option<Vec<String>> {
    let mut ids: Option<Vec<String>> = None;
    let mut take_next = false;
    for arg in args {
        if take_next {
            ids = Some(split_ids(arg));
            take_next = false;
        } else if let Some(value) = arg.strip_prefix("--probe-events=") {
            ids = Some(split_ids(value));
        } else if arg == "--probe-events" {
            take_next = true;
        }
    }
    ids
}

fn split_ids(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|id| id.trim().to_string())
        .filter(|id| !id.is_empty())
        .collect()
}

impl PresentProbe {
    /// Build from the process environment + CLI: tracked ids from
    /// `--probe-events` (CLI) falling back to `CZS_UI_PROBE_EVENTS`;
    /// sink from `CZS_UI_SIDECAR` (append) or stdout. Empty id list ⇒
    /// inactive. Emits one `armed` line when active.
    pub fn from_env() -> Self {
        let args: Vec<String> = std::env::args().skip(1).collect();
        let ids = tracked_from_args(&args).or_else(|| {
            std::env::var("CZS_UI_PROBE_EVENTS").ok().map(|raw| {
                let mut ids = split_ids(&raw);
                ids.sort();
                ids
            })
        });
        let Some(ids) = ids else { return Self::default() };
        if ids.is_empty() {
            return Self::default();
        }
        let sink = match std::env::var("CZS_UI_SIDECAR") {
            Ok(path) if !path.is_empty() => {
                match std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                {
                    Ok(file) => Sink::File(file),
                    Err(error) => {
                        eprintln!(
                            "present-probe: cannot open CZS_UI_SIDECAR \
                             {path}: {error} — falling back to stdout"
                        );
                        Sink::Stdout
                    }
                }
            }
            _ => Sink::Stdout,
        };
        let mut inner = ProbeInner {
            states: ids.iter().cloned().map(|id| (id, Track::Waiting)).collect(),
            order: ids.clone(),
            sink,
        };
        inner.sink.write_line(
            &serde_json::json!({
                "probe": "present",
                "event": "armed",
                "probe_layer": PROBE_LAYER,
                "tracked": ids,
            })
            .to_string(),
        );
        PresentProbe {
            shared: Some(Arc::new(Mutex::new(inner))),
        }
    }

    /// Active = at least one tracked id configured.
    pub fn is_active(&self) -> bool {
        self.shared.is_some()
    }

    /// Called after every logic tick (and after the baseline connect):
    /// Waiting → Applied once the reducer ingested the id; Applied →
    /// Waiting again if a reset discarded the projection before any
    /// paint (an unmarked id must never be stamped against frames that
    /// no longer contain it); Marked is terminal.
    pub fn observe(
        &self,
        reducer: &Reducer,
        journal_enqueued_ns: Option<u128>,
    ) {
        let Some(shared) = &self.shared else { return };
        let Ok(mut inner) = shared.lock() else { return };
        // clone: `order` is borrowed while `states` mutates below
        for id in inner.order.clone() {
            let state = inner.states.get_mut(&id).expect("order ⊆ states");
            match *state {
                Track::Waiting => {
                    if reducer.saw_event(&id) {
                        *state = Track::Applied(journal_enqueued_ns);
                    }
                }
                Track::Applied(_) => {
                    if !reducer.saw_event(&id) {
                        // reset discarded the projection before a paint
                        *state = Track::Waiting;
                    }
                }
                Track::Marked => {}
            }
        }
    }

    /// Add the paint-callback shape as the LAST shape of the top
    /// (`Foreground`) layer, so the callback runs after every other
    /// draw command of the frame was recorded. No-op when inactive or
    /// when every tracked id is already marked.
    pub fn paint_probe(&self, ctx: &egui::Context, rect: egui::Rect) {
        let Some(shared) = &self.shared else { return };
        if let Ok(inner) = shared.lock() {
            if inner.all_marked() {
                return;
            }
        }
        let painter = egui::Painter::new(
            ctx.clone(),
            egui::LayerId::new(
                egui::Order::Foreground,
                // stable singleton layer identity: `Id::unique()` would
                // allocate a distinct layer every frame
                #[allow(deprecated)]
                egui::Id::new("present-probe"),
            ),
            rect,
        );
        painter.add(egui_wgpu::Callback::new_paint_callback(
            rect,
            ProbePaint {
                shared: Arc::clone(shared),
            },
        ));
    }
}

/// The backend paint callback: executes inside the frame's
/// `wgpu::RenderPass`, after tessellation, before submit/present.
/// Issues ZERO draw commands — it only stamps the clock.
struct ProbePaint {
    shared: Arc<Mutex<ProbeInner>>,
}

impl egui_wgpu::CallbackTrait for ProbePaint {
    fn paint(
        &self,
        _info: egui::PaintCallbackInfo,
        _render_pass: &mut egui_wgpu::wgpu::RenderPass<'static>,
        _callback_resources: &egui_wgpu::CallbackResources,
    ) {
        // same clock source as journal_enqueued_ns / ui_drawn_ns
        self.shared
            .lock()
            .map(|mut inner| inner.mark_frame_paint(super::monotonic_ns()))
            .ok();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// File-backed probe on a unique temp path; returns (probe, path).
    /// Uses the real sink path (open/append/flush) the sidecar uses.
    fn file_probe(ids: &[&str]) -> (PresentProbe, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "p12-present-probe-{}-{}.ndjson",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        let ids: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
        let mut inner = ProbeInner {
            states: ids.iter().cloned().map(|id| (id, Track::Waiting)).collect(),
            order: ids.clone(),
            sink: Sink::File(file),
        };
        inner.sink.write_line(
            &serde_json::json!({
                "probe": "present", "event": "armed",
                "probe_layer": PROBE_LAYER, "tracked": ids,
            })
            .to_string(),
        );
        (PresentProbe { shared: Some(Arc::new(Mutex::new(inner))) }, path)
    }

    fn read_lines(path: &std::path::PathBuf) -> Vec<serde_json::Value> {
        let text = std::fs::read_to_string(path).unwrap();
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    fn event(id: &str, stream: &str, seq: i64) -> serde_json::Value {
        json!({
            "v": 1, "event_id": id, "stream_id": stream, "seq": seq,
            "kind": "task.accepted", "task_id": stream,
        })
    }

    /// Drive the GUI-free core exactly like `ProbePaint::paint` does,
    /// but with an injected clock so assertions are deterministic.
    fn paint_mark(probe: &PresentProbe, now: u128) {
        probe
            .shared
            .as_ref()
            .expect("active probe")
            .lock()
            .unwrap()
            .mark_frame_paint(now);
    }

    #[test]
    fn first_paint_marked_once_and_only_after_reducer_ingest() {
        // CLI parse sanity (pure fn): `--probe-events=a,b`
        let ids =
            tracked_from_args(&["--probe-events=marker/1,marker/2".into()])
                .unwrap();
        assert_eq!(ids, vec!["marker/1", "marker/2"]);
        let ids = tracked_from_args(&["--probe-events".into(), "x".into()])
            .unwrap();
        assert_eq!(ids, vec!["x"]);

        let (probe, path) = file_probe(&["a/1/acc", "b/1/acc"]);
        let mut reducer = Reducer::new();

        // paint before any ingest: nothing marked, no lines
        paint_mark(&probe, 100);
        assert_eq!(read_lines(&path).len(), 1); // armed only

        // a ingested (live), b not yet
        reducer.apply_event(&event("a/1/acc", "a", 1));
        probe.observe(&reducer, Some(111));
        paint_mark(&probe, 200);
        let lines = read_lines(&path);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[1]["event_id"], "a/1/acc");
        assert_eq!(lines[1][MARK_KEY], json!(200));
        assert_eq!(lines[1]["journal_enqueued_ns"], json!(111));
        assert_eq!(lines[1]["probe_layer"], json!(PROBE_LAYER));

        // repeated paints never re-mark a; b still nothing
        paint_mark(&probe, 300);
        assert_eq!(read_lines(&path).len(), 2);

        // b arrives later (baseline replay has no journal mark → null)
        reducer.apply_event(&event("b/1/acc", "b", 1));
        probe.observe(&reducer, None);
        paint_mark(&probe, 400);
        let lines = read_lines(&path);
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[2]["event_id"], "b/1/acc");
        assert_eq!(lines[2][MARK_KEY], json!(400));
        assert_eq!(lines[2]["journal_enqueued_ns"], serde_json::Value::Null);
    }

    #[test]
    fn reset_demotes_unmarked_but_marked_is_terminal() {
        let (probe, path) = file_probe(&["a/1/acc", "b/1/acc"]);
        let mut reducer = Reducer::new();
        reducer.apply_event(&event("a/1/acc", "a", 1));
        probe.observe(&reducer, Some(50));
        paint_mark(&probe, 60); // a's frame painted
        assert_eq!(read_lines(&path).len(), 2); // armed + a

        // b ingested later, but NO frame painted yet
        reducer.apply_event(&event("b/1/acc", "b", 1));
        probe.observe(&reducer, Some(70));

        // disconnect/reset discards the projection: b is demoted, and a
        // paint NOW must not stamp b against frames without its state
        reducer.apply_reset();
        probe.observe(&reducer, Some(80));
        paint_mark(&probe, 90);
        assert_eq!(read_lines(&path).len(), 2); // no new marks

        // fresh baseline re-ingests b; a was already marked once and
        // stays terminal even though the new baseline re-saw it
        reducer.apply_event(&event("a/1/acc", "a", 1));
        reducer.apply_event(&event("b/1/acc", "b", 1));
        probe.observe(&reducer, Some(100));
        paint_mark(&probe, 110);
        let lines = read_lines(&path);
        assert_eq!(lines.len(), 3); // armed + a + b (no re-mark of a)
        assert_eq!(lines[2]["event_id"], "b/1/acc");
        assert_eq!(lines[2][MARK_KEY], json!(110));
    }
}
