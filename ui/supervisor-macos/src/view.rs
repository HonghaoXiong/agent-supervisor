//! The view model: pure functions from [`Projection`](crate::model::Projection)
//! to what the egui frame renders. No GUI types leak in — everything is
//! testable headless, and the [`crate::view_ui`] eframe layer only draws.
//!
//! Only VISIBLE rows are materialized (`visible_rows`): the task list may
//! be arbitrarily large; the view never maps it wholesale.

// `super::model` (not `crate::model`): view.rs is compiled both as a
// module of the binary crate and via #[path] includes from the
// integration tests, where `crate::` points at the test crate.
use super::model::{GapMarker, Health, Projection, TaskState};

/// Filter box contents (worker/status/kind, case-insensitive substring).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Filter {
    pub query: String,
}

/// Placeholder shown in the filter box while it is empty (ui-polish1
/// item 9 — the live session found no affordance at all).
pub const FILTER_HINT: &str = "filter tasks…";

/// Uniform vertical gap between visual sections (filter → list →
/// details → gap markers), px at 1× scale. ui-polish1 item 7: the live
/// session measured a 36/20px mix; every section boundary now uses this
/// ONE constant. (Intra-list row spacing stays the theme default —
/// uniform by construction.)
pub const SECTION_SPACING: f32 = 20.0;

/// Semantic display class of a task status word (ui-polish1 item 1).
/// The color is NEVER the sole carrier — the verbatim word always
/// renders beside the dot — so the classes stay distinguishable without
/// color vision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusKind {
    /// Accepted, nothing further observed yet.
    Accepted,
    /// Session/turn activity observed.
    Running,
    /// Terminal success.
    Completed,
    /// Terminal failure (`failed`, `timeout`).
    Failed,
    /// Terminal-neutral (`cancelled`) and unknown/forward-compat words.
    Neutral,
}

/// Semantic color for a [`StatusKind`] — mapped to a concrete theme-safe
/// [`egui::Color32`] in ONE place in the draw layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorRole {
    Green,
    Amber,
    Red,
    /// Neutral text tier of the current theme (no dedicated hue).
    Neutral,
}

impl StatusKind {
    pub fn color_role(self) -> ColorRole {
        match self {
            StatusKind::Accepted | StatusKind::Neutral => ColorRole::Neutral,
            StatusKind::Running => ColorRole::Amber,
            StatusKind::Completed => ColorRole::Green,
            StatusKind::Failed => ColorRole::Red,
        }
    }
}

/// Classify a status word. Pure: the draw layer, the footer counts and
/// the tests all share this one table.
pub fn status_kind(status: &str) -> StatusKind {
    match status {
        "accepted" => StatusKind::Accepted,
        "running" => StatusKind::Running,
        "completed" => StatusKind::Completed,
        "failed" | "timeout" => StatusKind::Failed,
        _ => StatusKind::Neutral, // cancelled + unknown words
    }
}

/// The single visual state a list row may show (ui-polish1 item 3: the
/// keyboard-only session observed leftover gray bands coexisting with
/// the blue selection). At most ONE state per row; `selected` always
/// wins, and `pointer_hover` must only ever come from a real pointer
/// position this frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowVisualState {
    Selected,
    Hovered,
    Plain,
}

pub fn row_visual_state(
    selected: bool,
    pointer_hover: bool,
) -> RowVisualState {
    if selected {
        RowVisualState::Selected
    } else if pointer_hover {
        RowVisualState::Hovered
    } else {
        RowVisualState::Plain
    }
}

/// The three text-gray tiers (ui-polish1 item 8): title > status line >
/// body, each defined exactly once. A tier is a blend factor toward the
/// panel background — hard-coded grays would vanish on the other theme
/// (the app follows the system light/dark preference), blend factors do
/// not.
pub const TEXT_TIER_TITLE: f32 = 0.00;
pub const TEXT_TIER_STATUS: f32 = 0.18;
pub const TEXT_TIER_BODY: f32 = 0.32;

/// Blend a foreground/background RGB pair toward the background by
/// `tier` (0.0 = full foreground). Pure, so the tier ladder is
/// unit-testable on both light and dark theme pairs.
pub fn tier_color(
    fg: (u8, u8, u8),
    bg: (u8, u8, u8),
    tier: f32,
) -> (u8, u8, u8) {
    let f = tier.clamp(0.0, 1.0);
    let ch = |c: u8, b: u8| {
        (c as f32 + (b as f32 - c as f32) * f)
            .round()
            .clamp(0.0, 255.0) as u8
    };
    (ch(fg.0, bg.0), ch(fg.1, bg.1), ch(fg.2, bg.2))
}

/// Footer summary counts over a set of status words (ui-polish1 item 5).
/// `other` collects cancelled/unknown words and is only rendered when
/// non-zero, so the common shape stays exactly
/// `running X · completed Y · accepted Z · failed W`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FooterCounts {
    pub running: usize,
    pub completed: usize,
    pub accepted: usize,
    pub failed: usize,
    pub other: usize,
}

pub fn footer_counts<'a>(
    statuses: impl IntoIterator<Item = &'a str>,
) -> FooterCounts {
    let mut c = FooterCounts::default();
    for status in statuses {
        match status {
            "running" => c.running += 1,
            "completed" => c.completed += 1,
            "accepted" => c.accepted += 1,
            "failed" | "timeout" => c.failed += 1,
            _ => c.other += 1,
        }
    }
    c
}

/// Footer counts over the FILTERED task set (the counts always match
/// the rows the user sees).
pub fn footer_of(projection: &Projection, filter: &Filter) -> FooterCounts {
    footer_counts(
        projection
            .order
            .iter()
            .filter_map(|id| projection.tasks.get(id))
            .filter(|task| filter.matches(task))
            .map(|task| task.status.as_str()),
    )
}

/// The footer line exactly as drawn (ui-polish1 item 5).
pub fn footer_text(counts: &FooterCounts) -> String {
    let mut text = format!(
        "running {} · completed {} · accepted {} · failed {}",
        counts.running, counts.completed, counts.accepted, counts.failed
    );
    if counts.other > 0 {
        text.push_str(&format!(" · other {}", counts.other));
    }
    text
}

/// One rendered row (subset of fields; details come from `details_of`).
/// `status` is the VERBATIM display word from the model — the renderer
/// never rewrites it (ui-polish1: the old `running: bool` flag collapsed
/// every non-terminal word to "running", which is how `accepted` came
/// to display as `running`).
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub task_id: String,
    pub worker: String,
    pub status: String,
}

/// Status banners surfaced above the list.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Banners {
    pub stale: bool,
    pub live: bool,
    pub gap_count: usize,
    pub unattributed_count: usize,
}

/// A safe detail line for the selected task (fixed keys, no raw payload).
#[derive(Debug, Clone, PartialEq)]
pub struct Detail {
    pub task_id: String,
    pub worker: String,
    pub mode: String,
    pub status: String,
    pub outcome: String,
    pub session_id: String,
    pub turn_id: String,
    pub turn_status: String,
    pub delivery_ok: String,
}

impl Filter {
    pub fn matches(&self, task: &TaskState) -> bool {
        if self.query.is_empty() {
            return true;
        }
        let q = self.query.to_lowercase();
        task.task_id.to_lowercase().contains(&q)
            || task
                .worker
                .as_deref()
                .unwrap_or("")
                .to_lowercase()
                .contains(&q)
            || task.status.to_lowercase().contains(&q)
    }
}

/// Materialize only the visible slice of the (filtered) task list.
pub fn visible_rows(
    projection: &Projection,
    filter: &Filter,
    offset: usize,
    count: usize,
) -> Vec<Row> {
    let mut rows: Vec<Row> = Vec::with_capacity(count);
    let mut skipped = 0usize;
    for task_id in &projection.order {
        let Some(task) = projection.tasks.get(task_id) else { continue };
        if !filter.matches(task) {
            continue;
        }
        if skipped < offset {
            skipped += 1;
            continue;
        }
        if rows.len() >= count {
            break;
        }
        rows.push(Row {
            task_id: task.task_id.clone(),
            worker: task.worker.clone().unwrap_or_default(),
            status: task.status.clone(),
        });
    }
    rows
}

/// Total rows matching the filter (drives the scroll-window clamp and
/// the "rows x-y of z" label; the list itself is never materialized).
pub fn filtered_count(projection: &Projection, filter: &Filter) -> usize {
    projection
        .order
        .iter()
        .filter(|id| {
            projection
                .tasks
                .get(*id)
                .map(|task| filter.matches(task))
                .unwrap_or(false)
        })
        .count()
}

/// Banner state derived from the projection.
pub fn banners(projection: &Projection) -> Banners {
    Banners {
        stale: projection.health == Some(Health::Stale),
        live: projection.health == Some(Health::Live),
        gap_count: projection.gaps.len(),
        unattributed_count: projection.unattributed_count,
    }
}

/// Safe per-task detail (never raw event payloads / initiator strings).
pub fn details_of(task: &TaskState) -> Detail {
    Detail {
        task_id: task.task_id.clone(),
        worker: task.worker.clone().unwrap_or_default(),
        mode: task.mode.clone().unwrap_or_default(),
        status: task.status.clone(),
        outcome: task.outcome.clone().unwrap_or_default(),
        session_id: task.session_id.clone().unwrap_or_default(),
        turn_id: task.turn_id.clone().unwrap_or_default(),
        turn_status: task.turn_status.clone().unwrap_or_default(),
        delivery_ok: match task.delivery_ok {
            Some(true) => "true".into(),
            Some(false) => "false".into(),
            None => String::new(),
        },
    }
}

/// Human summary of a gap marker (rendered as "unknown range", never
/// "history ends here").
pub fn gap_label(gap: &GapMarker) -> String {
    format!(
        "{}: seqs {}..{} unknown ({} events) — not complete history",
        gap.stream_id, gap.from_seq, gap.to_seq, gap.count
    )
}

/// Safe error text: fixed code + operator-safe message only.
pub fn safe_error(code: &str, message: &str) -> String {
    format!("error {code}: {message}")
}

/// The status-bar hub badge text — the ONE AND ONLY source of the
/// `hub: <name>` segment (polish1: the connection status line used to
/// embed a second `hub:` and the bar rendered the project name twice).
/// `None` (env / CWD-fallback / manual connects) renders no badge at
/// all. The text is IDENTICAL whether the badge renders as a plain
/// label (single instance) or as the cycle button (multi instance) —
/// only the interactivity differs, never the text.
pub fn hub_badge_text(hub_label: Option<&str>) -> Option<String> {
    hub_label
        .filter(|label| !label.is_empty())
        .map(|label| format!("hub: {label}"))
}

/// Pure join of the status-bar text exactly as `ui` composes it
/// (status line widget + badge widget; banners/gaps follow). Tests pin
/// the SHAPE here: whatever the connection state, the bar carries the
/// `hub:` prefix at most once — through the badge, never through the
/// connection status line itself.
pub fn status_bar_text(status_line: &str, hub_label: Option<&str>) -> String {
    match hub_badge_text(hub_label) {
        Some(badge) if !status_line.is_empty() => {
            format!("{status_line}  {badge}")
        }
        Some(badge) => badge,
        None => status_line.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::model::{Projection, TaskState};
    use super::*;

    fn task(id: &str, status: &str, worker: &str) -> TaskState {
        TaskState {
            task_id: id.into(),
            worker: Some(worker.into()),
            status: status.into(),
            ..Default::default()
        }
    }

    fn projection_with(tasks: Vec<TaskState>) -> Projection {
        let mut p = Projection::new();
        for t in tasks {
            p.order.push(t.task_id.clone());
            p.tasks.insert(t.task_id.clone(), t);
        }
        p
    }

    #[test]
    fn visible_rows_window_and_filter() {
        let tasks: Vec<TaskState> = (0..50)
            .map(|i| task(&format!("t{i}"), "running", "codex"))
            .collect();
        let projection = projection_with(tasks);
        let rows = visible_rows(&projection, &Filter::default(), 10, 5);
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].task_id, "t10");
        let filtered = visible_rows(
            &projection,
            &Filter { query: "t4".into() },
            0,
            100,
        );
        assert!(filtered.iter().all(|r| r.task_id.contains("t4")));
    }

    #[test]
    fn row_status_is_verbatim_display_word() {
        // ui-polish1: the old `running` flag rewrote every non-terminal
        // word to "running" at draw time; rows now carry the model word
        // verbatim — `accepted` displays as `accepted`.
        for (word, expected) in [
            ("accepted", "accepted"),
            ("running", "running"),
            ("completed", "completed"),
            ("failed", "failed"),
        ] {
            let projection =
                projection_with(vec![task("a", word, "codex")]);
            let rows =
                visible_rows(&projection, &Filter::default(), 0, 10);
            assert_eq!(rows[0].status, expected);
        }
        let projection = projection_with(vec![task("a", "completed", "codex")]);
        let detail = details_of(projection.tasks.get("a").unwrap());
        assert_eq!(detail.status, "completed");
        assert!(detail.outcome.is_empty());
    }

    #[test]
    fn hub_badge_is_the_single_hub_segment() {
        // polish1 duplicate-label contract: the badge is the ONLY
        // `hub:` segment; the connection status line never embeds one.
        assert_eq!(
            hub_badge_text(Some("czs-demo-proj")).as_deref(),
            Some("hub: czs-demo-proj")
        );
        assert_eq!(hub_badge_text(None), None);
        assert_eq!(hub_badge_text(Some("")), None);
        let bar = status_bar_text("connected: 0 tasks", Some("czs-demo-proj"));
        assert_eq!(bar.matches("hub:").count(), 1, "exactly one: {bar}");
        assert!(bar.contains("hub: czs-demo-proj"));
        // no registry label (env / CWD fallback / manual): no badge
        assert_eq!(
            status_bar_text("connected: 3 tasks", None),
            "connected: 3 tasks"
        );
        // even a hub-bearing status LINE would duplicate — the join
        // must not paper over it (guards the app against regression)
        assert_eq!(
            status_bar_text("connected: 0 tasks · hub: czs-demo-proj",
                            Some("czs-demo-proj"))
                .matches("hub:")
                .count(),
            2,
            "an embedded hub label WOULD duplicate — the app must not emit one"
        );
    }

    // -- ui-polish1 pure-function contracts --------------------------------

    #[test]
    fn status_kinds_and_roles() {
        assert_eq!(status_kind("accepted"), StatusKind::Accepted);
        assert_ne!(status_kind("accepted"), StatusKind::Running);
        assert_eq!(status_kind("running"), StatusKind::Running);
        assert_eq!(status_kind("completed"), StatusKind::Completed);
        assert_eq!(status_kind("failed"), StatusKind::Failed);
        assert_eq!(status_kind("timeout"), StatusKind::Failed);
        assert_eq!(status_kind("cancelled"), StatusKind::Neutral);
        assert_eq!(status_kind("anything-new"), StatusKind::Neutral);
        // dispatch palette: completed=green, running=amber,
        // accepted=neutral gray, failed=red
        assert_eq!(
            StatusKind::Completed.color_role(),
            ColorRole::Green
        );
        assert_eq!(StatusKind::Running.color_role(), ColorRole::Amber);
        assert_eq!(
            StatusKind::Accepted.color_role(),
            ColorRole::Neutral
        );
        assert_eq!(StatusKind::Failed.color_role(), ColorRole::Red);
        // neutral hue only ever comes from the theme text tier
        assert_eq!(StatusKind::Neutral.color_role(), ColorRole::Neutral);
    }

    #[test]
    fn row_visual_state_single_state_with_priority() {
        // at most ONE state; selected wins over hover (item 3)
        assert_eq!(
            row_visual_state(true, true),
            RowVisualState::Selected
        );
        assert_eq!(
            row_visual_state(true, false),
            RowVisualState::Selected
        );
        assert_eq!(
            row_visual_state(false, true),
            RowVisualState::Hovered
        );
        assert_eq!(row_visual_state(false, false), RowVisualState::Plain);
    }

    #[test]
    fn footer_counts_and_text() {
        // accepted counts SEPARATELY from running (the display defect
        // the live session found must not survive in the footer either)
        let counts = footer_counts([
            "running",
            "running",
            "completed",
            "accepted",
            "accepted",
            "failed",
            "timeout",
        ]);
        assert_eq!(
            counts,
            FooterCounts {
                running: 2,
                completed: 1,
                accepted: 2,
                failed: 2, // failed + timeout
                other: 0,
            }
        );
        assert_eq!(
            footer_text(&counts),
            "running 2 · completed 1 · accepted 2 · failed 2"
        );
        // cancelled / unknown are counted, and surface only when set
        let odd = footer_counts(["cancelled", "weird"]);
        assert_eq!(odd.other, 2);
        assert_eq!(
            footer_text(&odd),
            "running 0 · completed 0 · accepted 0 · failed 0 · other 2"
        );
        // empty list: fixed four-segment shape, no "other"
        assert_eq!(
            footer_text(&FooterCounts::default()),
            "running 0 · completed 0 · accepted 0 · failed 0"
        );
    }

    #[test]
    fn footer_follows_the_filter() {
        let projection = projection_with(vec![
            task("a", "running", "codex"),
            task("b", "accepted", "codex"),
            task("c", "completed", "zcode"),
        ]);
        let all = footer_of(&projection, &Filter::default());
        assert_eq!(all.accepted, 1);
        assert_eq!(all.running, 1);
        assert_eq!(all.completed, 1);
        let filtered =
            footer_of(&projection, &Filter { query: "codex".into() });
        assert_eq!(filtered.completed, 0);
        assert_eq!(filtered.running, 1);
    }

    #[test]
    fn text_tiers_blend_monotonically_on_both_themes() {
        let light_fg = (32, 32, 32);
        let light_bg = (248, 248, 248);
        let dark_fg = (230, 230, 230);
        let dark_bg = (27, 27, 32);
        for (fg, bg) in [(light_fg, light_bg), (dark_fg, dark_bg)] {
            let title = tier_color(fg, bg, TEXT_TIER_TITLE);
            let status = tier_color(fg, bg, TEXT_TIER_STATUS);
            let body = tier_color(fg, bg, TEXT_TIER_BODY);
            let luma = |c: (u8, u8, u8)| {
                c.0 as i32 * 299 + c.1 as i32 * 587 + c.2 as i32 * 114
            };
            // 标题 > 状态行 > 正文: contrast against the background
            // decreases strictly down the ladder, on BOTH themes
            let d_title = (luma(title) - luma(bg)).abs();
            let d_status = (luma(status) - luma(bg)).abs();
            let d_body = (luma(body) - luma(bg)).abs();
            assert!(d_title > d_status,
                "title tier must outrank status tier");
            assert!(d_status > d_body,
                "status tier must outrank body tier");
            // tiers are distinct and never cross the background
            assert_ne!(title, status);
            assert_ne!(status, body);
        }
        // endpoint sanity: tier 0 is the foreground, tier 1 the bg
        assert_eq!(tier_color((10, 20, 30), (200, 210, 220), 0.0),
                   (10, 20, 30));
        assert_eq!(tier_color((10, 20, 30), (200, 210, 220), 1.0),
                   (200, 210, 220));
    }

    #[test]
    fn polish_constants_are_pinned() {
        assert_eq!(FILTER_HINT, "filter tasks…");
        assert_eq!(SECTION_SPACING, 20.0);
    }
}
