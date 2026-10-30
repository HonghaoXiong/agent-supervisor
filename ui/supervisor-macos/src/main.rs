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
//!
//! polish1 request deadline (P3.3 N2): the connect/baseline phase
//! (hello → snapshot → snapshot-events → subscribe) waits per op under
//! `ipc::REQUEST_DEADLINE` — a hub that accepts and then stays silent
//! fails the round bounded and the app returns to the waiting rescan
//! (startup can never stay wedged). The post-handshake EVENT STREAM
//! keeps NO idle deadline: protocol v1 has no heartbeat, a quiet
//! stream is legal, and its silence is undetectable at this layer
//! (v2 heartbeat = separate protocol work).

pub mod discovery;
pub mod ipc;
pub mod model;
pub mod probe;
pub mod view;

use eframe::egui;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use discovery::DiscoveryOutcome;
use ipc::{Frame, UiClient, CHANNEL_BOUND};
use model::Reducer;
use probe::PresentProbe;
use view::Filter;

const VISIBLE_ROWS: usize = 40;
/// Gap markers rendered per frame (the stored vec is bounded at
/// `model::MAX_GAP_MARKERS`; the view shows a summary beyond this).
const VISIBLE_GAP_MARKERS: usize = 8;
/// Main status line while no hub is discoverable (one-click attach:
/// keep rescanning instead of showing a raw connect error).
const WAITING_STATUS: &str = "waiting for a supervisor hub… (auto-attach on)";

/// The eframe application state.
pub struct SupervisorApp {
    pub reducer: Reducer,
    receiver: Option<Receiver<Frame>>,
    pub filter: Filter,
    pub selected: Option<String>,
    pub status_line: String,
    offset: usize,
    /// Current/last connect target (public: integration tests assert
    /// which hub a round actually connected to).
    pub socket: String,
    /// True when the current reconnect target was pinned by an explicit
    /// `connect()` call (tests / scripted paths): reconnects retry that
    /// exact socket instead of re-running discovery.
    socket_manual: bool,
    /// Registry-derived label of the CONNECTED hub (project dir name);
    /// `None` for env / CWD-fallback / manual connections.
    pub hub_label: Option<String>,
    /// Live registry entries as of the last discovery round (drives the
    /// cycle-to-next-hub control; empty unless registry-discovered).
    pub hub_candidates: Vec<discovery::HubEntry>,
    /// Hub-cycling target: the socket the next round should prefer.
    preferred_socket: Option<String>,
    /// No hub discoverable right now: bounded rescan posture.
    pub waiting: bool,
    /// Per-request read budget for the connect/baseline phase
    /// (hello / snapshot / snapshot-events / subscribe). Production
    /// default [`ipc::REQUEST_DEADLINE`]; tests shrink it so deadline
    /// paths run in milliseconds (polish1 P3.3 N2 — a silently lying
    /// server must fail the connect bounded, never block startup).
    pub connect_deadline: Duration,
    /// Discovery round injectable for deterministic tests; `None` uses
    /// the real env → registry → CWD resolution (`discovery::discover`).
    pub discover_fn:
        Option<Box<dyn Fn() -> DiscoveryOutcome + Send>>,
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
            status_line: WAITING_STATUS.into(),
            offset: 0,
            socket: String::new(),
            socket_manual: false,
            hub_label: None,
            hub_candidates: Vec::new(),
            preferred_socket: None,
            waiting: true,
            connect_deadline: ipc::REQUEST_DEADLINE,
            discover_fn: None,
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
    /// Connect + baseline + subscribe against an explicitly given
    /// socket: pins it as the reconnect target (legacy/test entry —
    /// production startup goes through `reconnect_round` discovery).
    pub fn connect(&mut self, socket_path: &str) -> Result<(), String> {
        self.connect_source(socket_path, true, None, Vec::new())
    }

    /// The shared connect path. `manual` pins the socket for retries;
    /// `hub_label`/`candidates` describe a registry-discovered hub.
    fn connect_source(
        &mut self,
        socket_path: &str,
        manual: bool,
        hub_label: Option<String>,
        candidates: Vec<discovery::HubEntry>,
    ) -> Result<(), String> {
        self.socket = socket_path.to_string(); // reconnect target
        self.socket_manual = manual;
        self.hub_label = hub_label;
        self.hub_candidates = candidates;
        // request-phase deadline (polish1 P3.3 N2): a server that
        // accepts and never answers fails here bounded instead of
        // wedging the round (and app startup) forever
        let (mut client, hello) =
            UiClient::connect_with_request_deadline(
                socket_path,
                self.connect_deadline,
            )
            .map_err(|e| e.0)?;
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
        self.waiting = false;
        // polish1 single-source contract: the status line carries the
        // CONNECTION facts only; the hub name lives exclusively in the
        // status-bar badge (view::hub_badge_text) so the bar renders
        // `hub:` exactly once (the old embedded copy duplicated it).
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
            self.reconnect_round(now);
        }
    }

    /// One full discovery round + connect attempt: env → registry → CWD
    /// fallback (re-resolved EVERY round, so a hub that changed project
    /// or socket is followed automatically). A manual pin (explicit
    /// `connect`) retries that exact socket instead. With no hub
    /// anywhere the app enters the bounded waiting rescan.
    pub fn reconnect_round(&mut self, now: Instant) {
        if self.socket_manual && !self.socket.is_empty() {
            let socket = self.socket.clone();
            match self.connect_source(&socket, true, None, Vec::new()) {
                Ok(()) => {
                    self.reconnect_attempts = 0;
                    self.waiting = false;
                }
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
            return;
        }
        let target: Option<(String, Option<String>)> =
            match self.run_discovery() {
                DiscoveryOutcome::Env(path) => {
                    self.hub_candidates.clear();
                    Some((path, None))
                }
                DiscoveryOutcome::Registry(live) => {
                    let index = discovery::choose_index(
                        &live,
                        self.preferred_socket.as_deref(),
                    );
                    self.preferred_socket = None; // consumed this round
                    let chosen = live[index].clone();
                    let label =
                        discovery::project_name(&chosen.project_root);
                    self.hub_candidates = live;
                    Some((chosen.socket, label))
                }
                DiscoveryOutcome::Fallback(path) => {
                    self.hub_candidates.clear();
                    Some((path, None))
                }
                DiscoveryOutcome::None => None,
            };
        let Some((socket, label)) = target else {
            // nothing anywhere: waiting posture, bounded rescan
            self.waiting = true;
            self.hub_candidates.clear();
            self.hub_label = None;
            self.status_line = WAITING_STATUS.into();
            self.reconnect_attempts += 1;
            self.reconnect_at = Some(
                now + discovery::rescan_backoff(self.reconnect_attempts),
            );
            return;
        };
        let candidates = std::mem::take(&mut self.hub_candidates);
        match self.connect_source(
            &socket,
            false,
            label.clone(),
            candidates,
        ) {
            Ok(()) => {
                self.reconnect_attempts = 0;
                self.waiting = false;
            }
            Err(error) => {
                // the discovered hub vanished mid-round (or refused):
                // keep the auto-attach posture and rescan bounded
                self.reducer.apply_reset();
                self.waiting = true;
                self.reconnect_attempts += 1;
                self.reconnect_at = Some(
                    now + discovery::rescan_backoff(
                        self.reconnect_attempts,
                    ),
                );
                self.status_line = format!(
                    "{WAITING_STATUS} ({error})"
                );
            }
        }
    }

    /// The injected-or-real discovery round (tests pin deterministic
    /// outcomes; production reads env / registry / CWD).
    ///
    /// Injection-surface contract (polish1 N2): an EMPTY `Registry`
    /// outcome — impossible from `discovery::resolve`, possible from an
    /// injected `discover_fn` — is normalized to `None` HERE so every
    /// caller treats it as "nothing anywhere" (waiting posture). No
    /// code path may ever index into an empty candidate list.
    fn run_discovery(&self) -> DiscoveryOutcome {
        let outcome = match &self.discover_fn {
            Some(injected) => injected(),
            None => discovery::discover(),
        };
        if let DiscoveryOutcome::Registry(live) = &outcome {
            if live.is_empty() {
                return DiscoveryOutcome::None;
            }
        }
        outcome
    }

    /// Minimal multi-hub switch (no list popup): advance to the next
    /// live registry entry and reconnect to it immediately. The
    /// voluntary switch reuses the observed-disconnect bookkeeping so
    /// the projection goes stale and re-baselines from the new hub.
    pub fn cycle_hub(&mut self) {
        if self.hub_candidates.len() < 2 {
            return;
        }
        let next = discovery::next_index(
            &self.hub_candidates,
            &self.socket,
        );
        self.preferred_socket =
            Some(self.hub_candidates[next].socket.clone());
        self.reducer.apply_reset();
        self.receiver = None;
        self.ui_client = None; // Drop joins the (already exited) reader
        self.reconnect_attempts = 0;
        self.reconnect_at = Some(Instant::now());
        self.status_line = "switching hub…".into();
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

// -- ui-polish1 draw helpers (single source for tiers and hues) ---------

/// Blend the theme text color toward the panel background by a
/// [`view::TEXT_TIER_*`] factor — one definition of the three-level
/// gray ladder (item 8), theme-safe in light AND dark mode.
pub(crate) fn text_color(ui: &egui::Ui, tier: f32) -> egui::Color32 {
    let fg = ui.visuals().text_color();
    let bg = ui.visuals().panel_fill;
    let c = view::tier_color(
        (fg.r(), fg.g(), fg.b()),
        (bg.r(), bg.g(), bg.b()),
        tier,
    );
    egui::Color32::from_rgb(c.0, c.1, c.2)
}

/// ONE place mapping semantic [`view::ColorRole`]s to concrete colors
/// (item 1). Mid-tone hues chosen to read on both the light and dark
/// themes the app follows; `Neutral` deliberately has no hue of its
/// own — it renders as the status text tier.
pub(crate) fn role_color(ui: &egui::Ui, role: view::ColorRole) -> egui::Color32 {
    match role {
        view::ColorRole::Green => egui::Color32::from_rgb(21, 132, 67),
        view::ColorRole::Amber => egui::Color32::from_rgb(176, 108, 9),
        view::ColorRole::Red => egui::Color32::from_rgb(196, 48, 48),
        view::ColorRole::Neutral => {
            text_color(ui, view::TEXT_TIER_STATUS)
        }
    }
}

/// A `key: value` line of the detail block — key in the status tier,
/// value in the body tier (item 8).
fn detail_line(ui: &mut egui::Ui, key: &str, value: &str) {
    ui.horizontal(|ui| {
        ui.label(egui::RichText::new(format!("{key}:")).color(
            text_color(ui, view::TEXT_TIER_STATUS),
        ));
        ui.label(egui::RichText::new(value).color(
            text_color(ui, view::TEXT_TIER_BODY),
        ));
    });
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
        self.draw(ui);
        // Present probe LAST: the paint callback is registered as the
        // final shape of the top layer, so it executes inside this
        // frame's render pass after every other draw command was
        // recorded (see probe.rs for the honesty boundary: this is a
        // pre-submit/pre-present mark, not `frame_submitted`).
        self.probe.paint_probe(ui.ctx(), ui.max_rect());
    }
}

impl SupervisorApp {
    /// The frame's whole drawing, extracted from [`eframe::App::ui`]
    /// (ui-polish1) so the headless shape audit
    /// (`tests/render_shape_audit.rs`) can drive the EXACT production
    /// draw pass through `egui::Context::run_ui` with no window.
    pub fn draw(&mut self, ui: &mut egui::Ui) {
        // -- top status bar (title tier; item 8) ------------------------
        egui::Panel::top("status").show(ui, |ui| {
            ui.horizontal(|ui| {
                let title_tier = text_color(ui, view::TEXT_TIER_TITLE);
                let status_tier = text_color(ui, view::TEXT_TIER_STATUS);
                ui.label(
                    egui::RichText::new(&self.status_line)
                        .color(title_tier),
                );
                // one-click multi-hub: the badge is the SINGLE source
                // of `hub: <name>` (polish1 — the status line no
                // longer embeds a second copy). Single instance: plain
                // label. Multiple live hubs: the SAME text as a cycle
                // button (click switches to the next hub; the hover
                // hint carries the affordance, the text stays stable).
                if let Some(badge) =
                    view::hub_badge_text(self.hub_label.as_deref())
                {
                    let badge_text =
                        egui::RichText::new(badge.clone())
                            .color(status_tier);
                    if self.hub_candidates.len() > 1 {
                        if ui
                            .selectable_label(false, badge_text)
                            .on_hover_text(
                                "click to switch to the next hub",
                            )
                            .clicked()
                        {
                            self.cycle_hub();
                        }
                    } else {
                        ui.label(badge_text);
                    }
                }
                let banners = view::banners(&self.reducer.projection);
                if banners.stale {
                    ui.colored_label(
                        role_color(ui, view::ColorRole::Red),
                        "stale/reconnecting",
                    );
                } else if banners.live {
                    ui.colored_label(
                        role_color(ui, view::ColorRole::Green),
                        "live",
                    );
                }
                ui.label(egui::RichText::new(format!(
                    "gaps: {}",
                    banners.gap_count
                ))
                .color(status_tier));
                ui.label(egui::RichText::new(format!(
                    "unattributed: {}",
                    banners.unattributed_count
                ))
                .color(status_tier));
            });
        });
        // -- bottom footer summary (item 5: no large dead space below
        //    the list — the counts always mirror the FILTERED set) ----
        egui::Panel::bottom("summary").show(ui, |ui| {
            let counts =
                view::footer_of(&self.reducer.projection, &self.filter);
            ui.label(egui::RichText::new(view::footer_text(&counts)).color(
                text_color(ui, view::TEXT_TIER_STATUS),
            ));
        });
        egui::CentralPanel::default().show(ui, |ui| {
            // -- filter row (items 2 + 9: hint text + visible focus) --
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("filter:").color(
                    text_color(ui, view::TEXT_TIER_STATUS),
                ));
                let edit = ui.add(
                    egui::TextEdit::singleline(&mut self.filter.query)
                        .hint_text(view::FILTER_HINT)
                        .desired_width(f32::INFINITY),
                );
                // Focus ring painted AFTER the widget so nothing can
                // cover it: 2px theme-selection stroke when focused,
                // the theme's ordinary 1px border otherwise. The caret
                // itself is stock egui behavior; this ring is the
                // always-visible focus affordance the live session
                // found missing.
                let visuals = ui.style().visuals.clone();
                let stroke = if edit.has_focus() {
                    egui::Stroke::new(
                        2.0,
                        visuals.selection.stroke.color,
                    )
                } else {
                    egui::Stroke::new(
                        1.0,
                        visuals.widgets.noninteractive.bg_stroke.color,
                    )
                };
                ui.painter().rect_stroke(
                    edit.rect,
                    4,
                    stroke,
                    egui::StrokeKind::Inside,
                );
            });
            ui.add_space(view::SECTION_SPACING);
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
                    ui.label(egui::RichText::new(format!(
                        "rows {}-{} of {}",
                        self.offset + 1,
                        (self.offset + rows.len()).min(total),
                        total
                    ))
                    .color(text_color(ui, view::TEXT_TIER_STATUS)));
                });
            }
            // -- task rows (items 1 + 3) ---------------------------------
            for row in &rows {
                let is_selected = self
                    .selected
                    .as_deref()
                    == Some(row.task_id.as_str());
                // status dot + verbatim word, hue from the shared
                // table; the word stays beside the dot so color is
                // never the sole carrier
                let status_color = role_color(
                    ui,
                    view::status_kind(&row.status).color_role(),
                );
                let atoms = (
                    egui::RichText::new(format!(
                        "{}  [{}]  ",
                        row.task_id, row.worker
                    ))
                    .color(text_color(ui, view::TEXT_TIER_BODY)),
                    egui::RichText::new(format!("● {}", row.status))
                        .color(status_color),
                );
                let mut button = egui::Button::selectable(is_selected, atoms);
                if !is_selected {
                    // item 3: an unselected row paints NO frame in any
                    // interaction state — the residual gray bands were
                    // the stock button frame's hover/active styles. The
                    // selection band stays the ONLY row band.
                    button = button
                        .fill(egui::Color32::TRANSPARENT)
                        .stroke(egui::Stroke::NONE);
                }
                let response = ui.add(button);
                // one visual state per row, selected first; a genuine
                // hover renders cursor-only (no paint), so a stale
                // hover position can never leave a band behind
                let _visual =
                    view::row_visual_state(is_selected, response.hovered());
                if response.clicked() {
                    self.selected = Some(row.task_id.clone());
                }
            }
            ui.add_space(view::SECTION_SPACING);
            // -- selected-task details -----------------------------------
            if let Some(sel) = &self.selected {
                if let Some(task) = self.reducer.projection.tasks.get(sel) {
                    let d = view::details_of(task);
                    detail_line(ui, "task", &d.task_id);
                    detail_line(ui, "worker", &d.worker);
                    detail_line(ui, "mode", &d.mode);
                    ui.horizontal(|ui| {
                        ui.label(egui::RichText::new("status:").color(
                            text_color(ui, view::TEXT_TIER_STATUS),
                        ));
                        ui.label(egui::RichText::new(&d.status).color(
                            role_color(
                                ui,
                                view::status_kind(&d.status).color_role(),
                            ),
                        ));
                    });
                    if !d.outcome.is_empty() {
                        detail_line(ui, "outcome", &d.outcome);
                    }
                    if !d.delivery_ok.is_empty() {
                        detail_line(ui, "delivery_ok", &d.delivery_ok);
                    }
                    if !d.session_id.is_empty() {
                        detail_line(ui, "session", &d.session_id);
                    }
                    if !d.turn_id.is_empty() {
                        detail_line(ui, "turn", &d.turn_id);
                    }
                }
            }
            ui.add_space(view::SECTION_SPACING);
            // bounded gap display: a few markers + honest summary line
            for gap in self
                .reducer
                .projection
                .gaps
                .iter()
                .take(VISIBLE_GAP_MARKERS)
            {
                ui.colored_label(
                    role_color(ui, view::ColorRole::Amber),
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
                ui.label(egui::RichText::new(format!(
                    "… +{extra_gaps} more gap markers"
                ))
                .color(text_color(ui, view::TEXT_TIER_STATUS)));
            }
        });
    }
}

fn main() -> eframe::Result<()> {
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
            // one-click startup: one full discovery round (env →
            // registry → CWD). Found → connect immediately; nothing
            // anywhere → the waiting rescan takes over and auto-attaches
            // when a hub appears (CZS_UI_SOCKET remains a hard override)
            app.reconnect_round(Instant::now());
            Ok(Box::new(app))
        }),
    )
}
