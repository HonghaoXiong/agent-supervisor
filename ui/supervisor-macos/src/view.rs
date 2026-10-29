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

/// One rendered row (subset of fields; details come from `details_of`).
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub task_id: String,
    pub worker: String,
    pub status: String,
    pub running: bool,
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
            running: !is_terminal(task),
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

fn is_terminal(task: &TaskState) -> bool {
    matches!(
        task.status.as_str(),
        "completed" | "failed" | "timeout" | "cancelled"
    )
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
    fn running_flag_and_details() {
        let projection =
            projection_with(vec![task("a", "completed", "codex")]);
        let rows = visible_rows(&projection, &Filter::default(), 0, 10);
        assert!(!rows[0].running);
        let detail = details_of(projection.tasks.get("a").unwrap());
        assert_eq!(detail.status, "completed");
        assert!(detail.outcome.is_empty());
    }
}
