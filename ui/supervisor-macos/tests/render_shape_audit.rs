//! Headless render shape audit (ui-polish1).
//!
//! The production draw pass — `SupervisorApp::draw`, extracted from
//! `eframe::App::ui` for exactly this purpose — is driven through
//! `egui::Context::run_ui` at the real 1280×800 window size with NO
//! window, and the painted shapes are audited directly. This is the
//! code-level self-verification for the rendering items a unit test
//! cannot see:
//!
//! * item 1 — every status word renders verbatim with its `●` dot and
//!   the shared hue table (green/amber/red present as text-format
//!   colors on the laid-out job);
//! * item 3 — exactly ONE selection band; no other opaque band fills;
//! * item 4 — no text is laid out past the window edges (an in-app
//!   source of "clipped glyph fragments" cannot exist);
//! * item 5 — the footer line renders with the exact pure-fn text;
//! * item 6 — nothing paints a narrow full-height strip at the window
//!   edge (the app has NO ScrollArea; this pins that none of its
//!   widgets introduces a scrollbar-like artifact either);
//! * item 9 — the filter placeholder renders while the query is empty.
//!
//! Read-only posture untouched: no window, no sockets — only what
//! `draw` emitted is inspected.

#[path = "../src/main.rs"]
mod app;

use eframe::egui;
use egui::{Pos2, Rect, Vec2};
use serde_json::json;

use app::view::{self, ColorRole};
use app::{role_color, SupervisorApp};

const SCREEN: Vec2 = Vec2::new(1280.0, 800.0);

fn screen_rect() -> Rect {
    Rect::from_min_size(Pos2::ZERO, SCREEN)
}

/// The demo-shaped mixed projection: one task per display state.
fn demo_app() -> SupervisorApp {
    let mut app = SupervisorApp::default();
    let accepted = |id: &str| {
        json!({
            "v": 1, "event_id": format!("{id}/1/acc"),
            "stream_id": id, "seq": 1, "kind": "task.accepted",
            "task_id": id, "worker": "codex", "mode": "app-server",
        })
    };
    let turn = |id: &str| {
        json!({
            "v": 1, "event_id": format!("{id}/2/turn"),
            "stream_id": id, "seq": 2, "kind": "turn.started",
            "task_id": id, "session_id": format!("s-{id}"),
            "turn_id": format!("turn-{id}"),
        })
    };
    let completed = |id: &str, status: &str| {
        json!({
            "v": 1, "event_id": format!("{id}/3/done"),
            "stream_id": id, "seq": 3, "kind": "task.completed",
            "task_id": id, "status": status, "outcome": "ok",
            "delivery_ok": true,
        })
    };
    let r = &mut app.reducer;
    r.apply_event(&accepted("task-alpha"));
    r.apply_event(&turn("task-alpha"));
    r.apply_event(&completed("task-alpha", "completed"));
    r.apply_event(&accepted("task-bravo"));
    r.apply_event(&turn("task-bravo"));
    r.apply_event(&accepted("task-charlie"));
    r.apply_event(&turn("task-charlie"));
    r.apply_event(&completed("task-charlie", "failed"));
    r.apply_event(&accepted("task-delta")); // stays accepted
    r.apply_event(&accepted("task-echo"));
    r.apply_event(&turn("task-echo"));
    r.apply_event(&completed("task-echo", "cancelled"));
    r.mark_live();
    app.selected = Some("task-bravo".into());
    app.status_line = "connected: 5 tasks".into();
    app
}

/// Run `draw` for two frames (ids/positions settle after the first)
/// and return the second frame's shapes plus the theme visuals in use.
fn render(
    mut app: SupervisorApp,
) -> (Vec<egui::epaint::ClippedShape>, egui::Visuals) {
    let ctx = egui::Context::default();
    let input = egui::RawInput {
        screen_rect: Some(screen_rect()),
        ..Default::default()
    };
    let mut visuals = None;
    for i in 0..2 {
        let out = ctx.run_ui(input.clone(), |ui| {
            if i == 1 {
                visuals = Some(ui.style().visuals.clone());
            }
            app.draw(ui);
        });
        if i == 1 {
            return (out.shapes, visuals.expect("visuals captured"));
        }
        out.drop_without_applying_deltas();
    }
    unreachable!("two frames always return")
}

fn all_text(shapes: &[egui::epaint::ClippedShape]) -> String {
    let mut text = String::new();
    for clipped in shapes {
        if let egui::Shape::Text(text_shape) = &clipped.shape {
            text.push_str(&text_shape.galley.job.text);
            text.push('\n');
        }
    }
    text
}

/// Every color the frame asked to paint: rect fills, rect strokes and
/// per-section text-format colors (pre-tessellation, so EXACT color
/// equality is meaningful).
fn all_colors(shapes: &[egui::epaint::ClippedShape]) -> Vec<egui::Color32> {
    let mut colors = Vec::new();
    for clipped in shapes {
        match &clipped.shape {
            egui::Shape::Rect(rs) => {
                colors.push(rs.fill);
                colors.push(rs.stroke.color);
            }
            egui::Shape::Text(text_shape) => {
                for section in &text_shape.galley.job.sections {
                    colors.push(section.format.color);
                }
            }
            _ => {}
        }
    }
    colors
}

/// Resolve the draw layer's own role hues on a throwaway context (the
/// chromatic ones are theme-independent constants in `role_color`).
fn expected_role_hues() -> [egui::Color32; 3] {
    let ctx = egui::Context::default();
    let mut hues = None;
    let out = ctx.run_pass(egui::RawInput::default(), |ctx| {
        hues = Some(ctx.root_ui(|ui| {
            [
                role_color(ui, ColorRole::Green),
                role_color(ui, ColorRole::Amber),
                role_color(ui, ColorRole::Red),
            ]
        }));
    });
    out.drop_without_applying_deltas();
    hues.expect("hues captured")
}

#[test]
fn audit_text_words_footer_and_hint() {
    let (shapes, _) = render(demo_app());
    let text = all_text(&shapes);
    // item 1: verbatim status words, each behind its dot — accepted is
    // NOT collapsed into running anywhere in the draw output
    for expect in [
        "● completed",
        "● running",
        "● failed",
        "● accepted",
        "● cancelled",
    ] {
        assert!(text.contains(expect), "missing {expect:?} in:\n{text}");
    }
    // item 5: footer line, exactly the pure function's text
    assert!(
        text.contains("running 1 · completed 1 · accepted 1 · failed 1 · other 1"),
        "footer text mismatch in:\n{text}"
    );
    // item 9: placeholder while the query is empty
    assert!(
        text.contains(view::FILTER_HINT),
        "filter hint missing in:\n{text}"
    );
    // the connection line still renders
    assert!(text.contains("connected: 5 tasks"), "status line: {text}");
}

#[test]
fn audit_single_selection_band_no_stray_fills() {
    let (shapes, visuals) = render(demo_app());
    // selection bands: opaque selection-colored rects wide enough to
    // be a row band — exactly ONE (the selected row); a second band is
    // precisely the "multi highlight residue" the session observed
    let bands: Vec<Rect> = shapes
        .iter()
        .filter_map(|c| match &c.shape {
            egui::Shape::Rect(rs) => Some(rs.clone()),
            _ => None,
        })
        .filter(|rs| {
            rs.fill == visuals.selection.bg_fill
                && rs.rect.width() > 40.0
                && rs.rect.height() > 10.0
        })
        .map(|rs| rs.rect)
        .collect();
    assert_eq!(
        bands.len(),
        1,
        "exactly one selection band (item 3); got {bands:?}"
    );
    // every opaque band-sized rect fill is from the known-good
    // widget/panel set — an unselected row's hover/active fill (the
    // residual gray band) would land here and fail the whitelist
    let allowed = [
        egui::Color32::TRANSPARENT,
        visuals.window_fill,
        visuals.panel_fill,
        visuals.faint_bg_color,
        visuals.extreme_bg_color,
        visuals.selection.bg_fill,
        visuals.widgets.noninteractive.bg_fill,
        visuals.widgets.inactive.bg_fill,
        visuals.widgets.inactive.weak_bg_fill,
        visuals.widgets.open.bg_fill,
    ];
    for rs in shapes.iter().filter_map(|c| match &c.shape {
        egui::Shape::Rect(rs) => Some(rs),
        _ => None,
    }) {
        if rs.fill != egui::Color32::TRANSPARENT
            && rs.rect.width() > 24.0
            && !allowed.contains(&rs.fill)
        {
            panic!(
                "unexpected opaque fill {:?} on {:?} — stray band? \
                 (item 3/6)",
                rs.fill, rs.rect
            );
        }
    }
}

#[test]
fn audit_nothing_paints_outside_or_along_the_right_edge() {
    let (shapes, _) = render(demo_app());
    let screen = screen_rect();
    for clipped in &shapes {
        // nothing paints outside the window at all
        assert!(
            screen.contains_rect(clipped.clip_rect),
            "clip rect outside the window: {:?}",
            clipped.clip_rect
        );
        // item 4: no text is ever laid out past the window — an
        // in-app source of "clipped glyph fragments" cannot exist
        if let egui::Shape::Text(text_shape) = &clipped.shape {
            let text_rect = Rect::from_min_size(
                text_shape.pos,
                text_shape.galley.rect.size(),
            );
            let overflow_right = text_rect.max.x - screen.max.x;
            assert!(
                overflow_right <= 1.0,
                "text runs past the right edge by {overflow_right}px: \
                 {:?}",
                text_shape.galley.job.text
            );
        }
        // item 6: no narrow, tall, opaque shape hugging the right
        // window edge — no in-app scrollbar-like strip exists (the app
        // has no ScrollArea; this pins that its widgets draw none)
        let bounds = match &clipped.shape {
            egui::Shape::Rect(rs) => Some(rs.rect),
            egui::Shape::Circle(c) => Some(Rect::from_center_size(
                c.center,
                Vec2::splat(2.0 * c.radius),
            )),
            _ => None,
        };
        let Some(bounds) = bounds else { continue };
        let edge_strip = bounds.min.x > screen.max.x - 24.0;
        let tall = bounds.height() > 0.6 * screen.height();
        let narrow = bounds.width() < 24.0;
        assert!(
            !(edge_strip && tall && narrow),
            "scrollbar-like strip painted at the right edge: {bounds:?}"
        );
    }
}

#[test]
fn audit_status_hues_are_the_shared_table() {
    let (shapes, _) = render(demo_app());
    let colors = all_colors(&shapes);
    let [green, amber, red] = expected_role_hues();
    // the three chromatic hues are painted (status dots + words)
    assert!(
        colors.contains(&green),
        "green hue {green:?} not painted (completed row)"
    );
    assert!(
        colors.contains(&amber),
        "amber hue {amber:?} not painted (running row)"
    );
    assert!(
        colors.contains(&red),
        "red hue {red:?} not painted (failed row)"
    );
    // the old neon palette must not come back anywhere
    assert!(
        !colors.contains(&egui::Color32::GREEN),
        "raw Color32::GREEN painted — use role_color"
    );
    assert!(
        !colors.contains(&egui::Color32::RED),
        "raw Color32::RED painted — use role_color"
    );
    assert!(
        !colors.contains(&egui::Color32::YELLOW),
        "raw Color32::YELLOW painted — use role_color"
    );
}

/// Item 3, the exact live-session failure mode: a pointer that was
/// once present and then went quiet keeps its last position in egui's
/// input state. Frame 1 settles; frame 2 moves the pointer over a task
/// row; frame 3 has NO pointer events at all (the "no mouse" state the
/// session captured). With the stock `selectable_label` styling that
/// stale hover painted a gray band beside the blue selection; the
/// polished rows paint no frame in any state, so frame 3 must contain
/// no band other than the single selection band.
#[test]
fn audit_stale_pointer_cannot_leave_a_band() {
    let mut app = demo_app();
    let ctx = egui::Context::default();
    let input = |events: Vec<egui::Event>| egui::RawInput {
        screen_rect: Some(screen_rect()),
        events,
        ..Default::default()
    };
    // frame 1: settle, and locate a row's on-screen rect
    let out = ctx.run_ui(input(vec![]), |ui| app.draw(ui));
    let row_center = out
        .shapes
        .iter()
        .find_map(|c| match &c.shape {
            egui::Shape::Text(t) if t.galley.job.text.starts_with("task-") => {
                Some(t.pos + t.galley.rect.size() * 0.5)
            }
            _ => None,
        })
        .expect("a task row rendered in frame 1");
    out.drop_without_applying_deltas();
    // frame 2: a real pointer visit over that row
    let out = ctx.run_ui(
        input(vec![egui::Event::PointerMoved(row_center)]),
        |ui| app.draw(ui),
    );
    out.drop_without_applying_deltas();
    // frame 3: NO pointer events — the pointer state is stale but
    // still "known". Audit this frame.
    let mut visuals = None;
    let out = ctx.run_ui(input(vec![]), |ui| {
        visuals = Some(ui.style().visuals.clone());
        app.draw(ui);
    });
    let (shapes, visuals) = (out.shapes.clone(), visuals.unwrap());
    out.drop_without_applying_deltas();
    let allowed = [
        egui::Color32::TRANSPARENT,
        visuals.window_fill,
        visuals.panel_fill,
        visuals.faint_bg_color,
        visuals.extreme_bg_color,
        visuals.selection.bg_fill,
        visuals.widgets.noninteractive.bg_fill,
        visuals.widgets.inactive.bg_fill,
        visuals.widgets.inactive.weak_bg_fill,
        visuals.widgets.open.bg_fill,
    ];
    for rs in shapes.iter().filter_map(|c| match &c.shape {
        egui::Shape::Rect(rs) => Some(rs),
        _ => None,
    }) {
        if rs.fill != egui::Color32::TRANSPARENT
            && rs.rect.width() > 24.0
            && !allowed.contains(&rs.fill)
        {
            panic!(
                "stale pointer left a band: fill {:?} on {:?} \
                 (hovered/active fills are not allowed anywhere)",
                rs.fill, rs.rect
            );
        }
    }
    // and still exactly one selection band
    let bands = shapes
        .iter()
        .filter_map(|c| match &c.shape {
            egui::Shape::Rect(rs) => Some(rs),
            _ => None,
        })
        .filter(|rs| {
            rs.fill == visuals.selection.bg_fill
                && rs.rect.width() > 40.0
                && rs.rect.height() > 10.0
        })
        .count();
    assert_eq!(bands, 1, "selection band count under stale pointer");
}
