//! Read-only supervisor UI — egui/eframe 0.37 vertical slice (P1.2).
//!
//! READ-ONLY by construction: this binary exposes connection info, task
//! lists, per-task details, filters, gap/unattributed/stale banners and
//! a simple timeline. It has NO approve/retry/stop/open-path/run-command
//! button or shortcut, talks only the four read-only UI ops, and never
//! writes to the hub.
//!
//! Standard startup does NOT require a task id: hello → (state pages,
//! event pages) → subscribe-all discovers running tasks by itself.
//!
//! eframe 0.37 app shape: `App::logic` runs the bounded IPC pump (no
//! painting, also while hidden); `App::ui` only draws. `request_repaint`
//! wakes the loop — a scheduler hint, NEVER a substitute for a present
//! probe (PLAN §5.3.5).
//!
//! Present probe (PLAN §5.3.5–§5.3.6, see `probe.rs`): when event ids
//! are tracked (`--probe-events` / `CZS_UI_PROBE_EVENTS`), a wgpu
//! paint callback registered as the last shape of the top layer stamps
//! `frame_paint_ns` per first-presented id into `CZS_UI_SIDECAR`
//! (or stdout). That layer is INSIDE the frame's render pass but
//! strictly BEFORE submit/present — eframe 0.37 has no app-visible
//! post-present callback — so the mark is honestly NOT
//! `frame_submitted` and E2E present latency stays `unavailable`.
//!
//! repair1 read-end lifecycle (see `ipc.rs` for the state-machine
//! diagram): Reset marker or channel Disconnected ⇒ reducer DISCARDS
//! the projection and goes stale; the logic pump then re-baselines via
//! hello → pages → subscribe on a bounded backoff schedule — a dead
//! socket can never become a retry storm. `ui_drawn_ns` is the app-side
//! draw-pass completion mark only; without a renderer/compositor
//! present probe, end-to-end latency stays honestly "unavailable".

pub mod ipc;
pub mod model;
pub mod probe;
pub mod view;

use eframe::egui;
use std::sync::mpsc::Receiver;
use std::time::Instant;

use ipc::{Frame, UiClient, CHANNEL_BOUND};
use model::Reducer;
use probe::PresentProbe;
use view::Filter;

/// Where the hub's owner-only socket lives (default P1.1 layout).
const DEFAULT_SOCKET: &str = ".czs/state/ui-events.sock";
const VISIBLE_ROWS: usize = 40;
/// Gap markers rendered per frame (the stored vec is bounded at
/// `model::MAX_GAP_MARKERS`; the view shows a summary beyond this).
const VISIBLE_GAP_MARKERS: usize = 8;

/// The eframe application state.
pub struct SupervisorApp {
    pub reducer: Reducer,
    receiver: Option<Receiver<Frame>>,
    pub filter: Filter,
    pub selected: Option<String>,
    pub status_line: String,
    offset: usize,
    socket: String,
    ui_client: Option<UiClient>,
    /// Bounded reconnect schedule: attempt count + earliest next try
    /// (public: the lifecycle tests assert the disconnect→reconnect
    /// path through `pump_with_clock`).
    pub reconnect_attempts: usize,
    pub reconnect_at: Option<Instant>,
    /// Monotonic sidecar (PLAN §5.3.5): same CLOCK_MONOTONIC source as
    /// the Python probes; `observed_at` never participates.
    pub journal_enqueued_ns: Option<u128>,
    /// App-side egui DRAW-pass completion mark (repair1 rename: this is
    /// NOT a renderer/compositor present probe; it only says the app
    /// finished building this frame's widgets).
    pub ui_drawn_ns: Option<u128>,
    /// Present probe (PLAN §5.3.5–§5.3.6): inactive unless ids are
    /// tracked via `--probe-events` / `CZS_UI_PROBE_EVENTS`. Emits
    /// `frame_paint_ns` (wgpu render-pass layer) per tracked id —
    /// honestly NOT `frame_submitted` (see `probe.rs`).
    pub probe: PresentProbe,
}

impl Default for SupervisorApp {
    fn default() -> Self {
        SupervisorApp {
            reducer: Reducer::new(),
            receiver: None,
            filter: Filter::default(),
            selected: None,
            status_line: "not connected".into(),
            offset: 0,
            socket: DEFAULT_SOCKET.into(),
            ui_client: None,
            reconnect_attempts: 0,
            reconnect_at: None,
            journal_enqueued_ns: None,
            ui_drawn_ns: None,
            probe: PresentProbe::default(),
        }
    }
}

impl SupervisorApp {
    /// Connect + baseline + subscribe (the whole standard startup).
    /// No task id required anywhere. A hello with `v != 1` is refused
    /// outright (repair1 #4: version drift may not feed the reducer).
    pub fn connect(&mut self, socket_path: &str) -> Result<(), String> {
        self.socket = socket_path.to_string(); // reconnect target
        let (mut client, hello) =
            UiClient::connect(socket_path).map_err(|e| e.0)?;
        self.reducer.note_hello();
        if client.protocol_version_mismatch {
            let server_v = hello
                .get("v")
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            // refused application IS a stale transition: nothing from a
            // version-drifted server may feed the reducer
            self.reducer.apply_reset();
            return Err(format!(
                "server protocol version {server_v} != 1 — refusing to apply"
            ));
        }
        let cursor = hello
            .get("initial_snapshot_cursor")
            .cloned()
            .ok_or("hello: no initial cursor")?;
        let states = ipc::fetch_all_pages(
            &mut client,
            "snapshot",
            &cursor,
            "states",
            256,
        )
        .map_err(|e| e.0)?;
        self.reducer.apply_state_pages(&states);
        let events = ipc::fetch_all_pages(
            &mut client,
            "snapshot-events",
            &cursor,
            "events",
            1024,
        )
        .map_err(|e| e.0)?;
        self.reducer.apply_event_pages(&events);
        let (receiver, _ack) =
            client.subscribe_all(&cursor).map_err(|e| e.0)?;
        self.receiver = Some(receiver);
        self.ui_client = Some(client);
        self.reducer.mark_live();
        self.status_line = format!("connected: {} tasks", states.len());
        // baseline events (event-page replay) may carry tracked ids
        self.probe.observe(&self.reducer, self.journal_enqueued_ns);
        Ok(())
    }

    /// Drain buffered subscribe frames into the reducer (bounded). Runs
    /// in `App::logic` — also while the window is hidden. A channel
    /// Disconnected (reader thread exited: server gone, protocol error,
    /// or overflow marker delivered) DISCARDS the projection, marks
    /// stale and schedules a bounded reconnect.
    pub fn pump(&mut self) {
        let Some(receiver) = &self.receiver else { return };
        let (frames, disconnected) =
            ipc::drain_frames(receiver, CHANNEL_BOUND.min(512));
        if !frames.is_empty() {
            self.journal_enqueued_ns = Some(monotonic_ns());
        }
        for frame in frames {
            match frame {
                Frame::Event(event) => self.reducer.apply_event(&event),
                Frame::Gap(gap) => self.reducer.apply_gap(&gap),
                Frame::Reset => self.reducer.apply_reset(),
                Frame::Heartbeat(_) => {}
                Frame::Response(_) => {}
            }
        }
        if disconnected {
            // reader thread is gone: nothing more can EVER arrive on
            // this connection — discard, flag stale, reconnect bounded
            self.reducer.apply_reset();
            self.receiver = None;
            self.ui_client = None; // Drop joins the (already exited) reader
            self.reconnect_attempts += 1;
            self.reconnect_at = Some(
                Instant::now()
                    + ipc::reconnect_backoff(self.reconnect_attempts),
            );
            self.status_line =
                "connection lost — stale, reconnecting (bounded)".into();
        }
    }

    /// The logic pump's reconnect decision, clock-injected so tests can
    /// drive disconnect → stale → re-baseline without a GUI. A failed
    /// attempt grows the backoff; success resets it. No retry storm.
    pub fn pump_with_clock(&mut self, now: Instant) {
        self.pump();
        if let Some(at) = self.reconnect_at {
            if now < at {
                return;
            }
            self.reconnect_at = None;
            let socket = self.socket.clone();
            match self.connect(&socket) {
                Ok(()) => self.reconnect_attempts = 0,
                Err(error) => {
                    self.reducer.apply_reset();
                    self.reconnect_attempts += 1;
                    self.reconnect_at = Some(
                        now + ipc::reconnect_backoff(
                            self.reconnect_attempts,
                        ),
                    );
                    self.status_line = view::safe_error(
                        "transport_unavailable",
                        &error,
                    );
                }
            }
        }
    }
}

/// Same-source monotonic clock as the Python probes (CLOCK_MONOTONIC;
/// only deltas participate, never `observed_at`).
pub fn monotonic_ns() -> u128 {
    use std::sync::OnceLock;
    static ORIGIN: OnceLock<std::time::Instant> = OnceLock::new();
    let origin = ORIGIN.get_or_init(std::time::Instant::now);
    origin.elapsed().as_nanos()
}

impl eframe::App for SupervisorApp {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.pump_with_clock(Instant::now());
        // probe scoping AFTER every mutation path (pump + any reconnect
        // outcome inside pump_with_clock): Applied/Waiting transitions
        // must reflect the reducer state the NEXT frame will draw
        self.probe.observe(&self.reducer, self.journal_enqueued_ns);
        // wake for the next channel batch; a hint, not a present probe
        ctx.request_repaint_after(std::time::Duration::from_millis(100));
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        // The DRAW pass marks its completion time for the sidecar —
        // honestly an app-side widget-build mark (renamed in repair1);
        // the renderer/compositor present probe does not exist yet, so
        // end-to-end latency remains "unavailable" (§5.3.5).
        self.ui_drawn_ns = Some(monotonic_ns());

        egui::Panel::top("status").show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(&self.status_line);
                let banners = view::banners(&self.reducer.projection);
                if banners.stale {
                    ui.colored_label(egui::Color32::RED, "stale/reconnecting");
                } else if banners.live {
                    ui.colored_label(egui::Color32::GREEN, "live");
                }
                ui.label(format!("gaps: {}", banners.gap_count));
                ui.label(
                    format!("unattributed: {}", banners.unattributed_count),
                );
            });
        });
        egui::CentralPanel::default().show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label("filter:");
                ui.text_edit_singleline(&mut self.filter.query);
            });
            let total =
                view::filtered_count(&self.reducer.projection, &self.filter);
            let max_offset = total.saturating_sub(VISIBLE_ROWS);
            self.offset = self.offset.min(max_offset);
            let rows = view::visible_rows(
                &self.reducer.projection,
                &self.filter,
                self.offset,
                VISIBLE_ROWS,
            );
            // >VISIBLE_ROWS scrolling (repair1 minor): explicit offset
            // controls with a live window label — no row is unreachable
            if total > VISIBLE_ROWS {
                ui.horizontal(|ui| {
                    if ui.button("up").clicked() {
                        self.offset = self.offset.saturating_sub(1);
                    }
                    if ui.button("down").clicked() {
                        self.offset = (self.offset + 1).min(max_offset);
                    }
                    ui.label(format!(
                        "rows {}-{} of {}",
                        self.offset + 1,
                        (self.offset + rows.len()).min(total),
                        total
                    ));
                });
            }
            ui.separator();
            for row in &rows {
                let selected =
                    self.selected.as_deref() == Some(row.task_id.as_str());
                if ui
                    .selectable_label(
                        selected,
                        format!(
                            "{}  [{}]  {}",
                            row.task_id,
                            row.worker,
                            if row.running { "running" } else { &row.status }
                        ),
                    )
                    .clicked()
                {
                    self.selected = Some(row.task_id.clone());
                }
            }
            ui.separator();
            if let Some(sel) = &self.selected {
                if let Some(task) = self.reducer.projection.tasks.get(sel) {
                    let d = view::details_of(task);
                    ui.label(format!("task: {}", d.task_id));
                    ui.label(format!("worker: {}", d.worker));
                    ui.label(format!("mode: {}", d.mode));
                    ui.label(format!("status: {}", d.status));
                    if !d.outcome.is_empty() {
                        ui.label(format!("outcome: {}", d.outcome));
                    }
                    if !d.delivery_ok.is_empty() {
                        ui.label(format!("delivery_ok: {}", d.delivery_ok));
                    }
                    if !d.session_id.is_empty() {
                        ui.label(format!("session: {}", d.session_id));
                    }
                    if !d.turn_id.is_empty() {
                        ui.label(format!(
                            "turn {} ({})",
                            d.turn_id, d.turn_status
                        ));
                    }
                }
            }
            // bounded gap display: a few markers + honest summary line
            for gap in self
                .reducer
                .projection
                .gaps
                .iter()
                .take(VISIBLE_GAP_MARKERS)
            {
                ui.colored_label(
                    egui::Color32::YELLOW,
                    view::gap_label(gap),
                );
            }
            let extra_gaps = self
                .reducer
                .projection
                .gaps
                .len()
                .saturating_sub(VISIBLE_GAP_MARKERS)
                + self.reducer.projection.dropped_gap_markers;
            if extra_gaps > 0 {
                ui.label(format!("… +{extra_gaps} more gap markers"));
            }
        });
        // Present probe LAST: the paint callback is registered as the
        // final shape of the top layer, so it executes inside this
        // frame's render pass after every other draw command was
        // recorded (see probe.rs for the honesty boundary: this is a
        // pre-submit/pre-present mark, not `frame_submitted`).
        self.probe.paint_probe(ui.ctx(), ui.max_rect());
    }
}

fn main() -> eframe::Result<()> {
    let socket = std::env::var("CZS_UI_SOCKET")
        .unwrap_or_else(|_| DEFAULT_SOCKET.into());
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 800.0]),
        ..Default::default()
    };
    eframe::run_native(
        "supervisor (read-only)",
        options,
        Box::new(move |_cc| {
            let mut app = SupervisorApp::default();
            // probe first: the baseline connect's event replay is
            // observed by `connect` itself
            app.probe = PresentProbe::from_env();
            app.socket = socket.clone();
            if let Err(error) = app.connect(&socket) {
                app.status_line =
                    view::safe_error("transport_unavailable", &error);
                app.reducer.apply_reset();
                app.reconnect_at =
                    Some(Instant::now() + ipc::reconnect_backoff(1));
            }
            Ok(Box::new(app))
        }),
    )
}
