//! Small rendering helpers shared by the views.

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Borders};

use super::theme;

/// A titled panel with rounded borders in the accent color.
pub fn panel(title: &str, accent: Color) -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title(Span::styled(format!(" {title} "), theme::accent(accent)))
        .border_style(Style::default().fg(accent))
        .style(Style::default().bg(theme::PANEL))
}

/// The title every tab's main panel shares:
/// `TAB · pipeline [count]  extra  extra`. The pipeline is the one whose
/// data the panel shows; lists carry their row count.
///
/// ```
/// use f5a::ui::widgets::panel_title;
///
/// let sort = "sort:cost↓".to_string();
/// assert_eq!(panel_title("HOTSPOTS", Some("p"), Some(3), &[sort]), "HOTSPOTS · p [3]  sort:cost↓");
/// assert_eq!(panel_title("METRICS", None, None, &[]), "METRICS");
/// ```
pub fn panel_title(
    tab: &str,
    pipeline: Option<&str>,
    count: Option<usize>,
    extras: &[String],
) -> String {
    let mut title = tab.to_string();
    if let Some(pipeline) = pipeline {
        title.push_str(&format!(" · {pipeline}"));
    }
    if let Some(count) = count {
        title.push_str(&format!(" [{count}]"));
    }
    for extra in extras.iter().filter(|extra| !extra.is_empty()) {
        title.push_str(&format!("  {extra}"));
    }
    title
}

/// The `sort:` part of a title, with the direction arrow.
pub fn sort_label(column: &str, is_descending: bool) -> String {
    format!("sort:{column}{}", if is_descending { "↓" } else { "↑" })
}

/// A label/value line for key-value panels.
pub fn kv_line(label: &str, value: String, value_style: Style) -> Line<'static> {
    Line::from(vec![
        Span::styled(format!(" {label:<14}"), theme::muted()),
        Span::styled(value, value_style),
    ])
}

/// Center a `width` x `height` box inside `area`, clamped to fit.
pub fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect {
        x: area.x + (area.width - width) / 2,
        y: area.y + (area.height - height) / 2,
        width,
        height,
    }
}

/// Split an area into evenly weighted horizontal columns.
pub fn columns<const N: usize>(area: Rect) -> [Rect; N] {
    let constraints = [Constraint::Ratio(1, N as u32); N];
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(constraints)
        .split(area);
    std::array::from_fn(|index| chunks[index])
}

/// A detail pane sits beside its table from this width on, below it from
/// `STACK_MIN_HEIGHT` rows on, and is hidden on smaller terminals.
const SIDE_BY_SIDE_MIN_WIDTH: u16 = 110;
const DETAIL_MIN_WIDTH: u16 = 46;
const STACK_MIN_HEIGHT: u16 = 24;
const STACKED_DETAIL_HEIGHT: u16 = 14;

/// Split `area` into a table and its detail pane: side by side when wide
/// (the table takes what it needs, the pane keeps its minimum), stacked
/// when tall, and the table alone otherwise.
pub fn split_beside(area: Rect, natural_table_width: u16) -> (Rect, Option<Rect>) {
    let (direction, constraints) = if area.width >= SIDE_BY_SIDE_MIN_WIDTH {
        let table_width = natural_table_width.min(area.width - DETAIL_MIN_WIDTH);
        (
            Direction::Horizontal,
            [Constraint::Length(table_width), Constraint::Min(0)],
        )
    } else if area.height >= STACK_MIN_HEIGHT {
        (
            Direction::Vertical,
            [
                Constraint::Min(0),
                Constraint::Length(STACKED_DETAIL_HEIGHT),
            ],
        )
    } else {
        return (area, None);
    };
    let parts = Layout::default()
        .direction(direction)
        .constraints(constraints)
        .split(area);
    (parts[0], Some(parts[1]))
}

/// First visible row of a table viewport: keeps `previous_offset` while the
/// selection stays on screen, and otherwise scrolls just enough to reveal it.
pub fn table_offset(
    selected: Option<usize>,
    count: usize,
    height: usize,
    previous_offset: usize,
) -> usize {
    if height == 0 || count <= height {
        return 0;
    }
    let selected = selected.unwrap_or(0).min(count - 1);
    let offset = if selected < previous_offset {
        selected
    } else if selected >= previous_offset + height {
        selected + 1 - height
    } else {
        previous_offset
    };
    offset.min(count - height)
}

/// Draw a vertical scrollbar on the right edge of `area` when needed.
pub fn scrollbar(
    frame: &mut ratatui::Frame,
    area: Rect,
    offset: usize,
    count: usize,
    height: usize,
) {
    if count <= height {
        return;
    }
    let mut state = ratatui::widgets::ScrollbarState::new(count.saturating_sub(height))
        .position(offset)
        .viewport_content_length(height);
    frame.render_stateful_widget(
        ratatui::widgets::Scrollbar::new(ratatui::widgets::ScrollbarOrientation::VerticalRight)
            .style(Style::default().fg(theme::MUTED))
            .thumb_style(Style::default().fg(theme::CYAN)),
        area,
        &mut state,
    );
}

/// A one-line horizontal bar of `width` cells filled to `fraction`.
pub fn bar(fraction: f64, width: usize) -> String {
    let fraction = fraction.clamp(0.0, 1.0);
    let cells = (fraction * width as f64).round() as usize;
    let mut bar = String::with_capacity(width * 3);
    for index in 0..width {
        bar.push(if index < cells { '█' } else { '·' });
    }
    bar
}

#[cfg(test)]
mod tests {
    use super::{bar, centered_rect, columns};
    use ratatui::layout::Rect;

    #[test]
    fn centered_rect_clamps_to_the_parent() {
        let parent = Rect::new(0, 0, 100, 40);
        let inner = centered_rect(60, 20, parent);
        assert_eq!(inner, Rect::new(20, 10, 60, 20));
        let oversized = centered_rect(200, 200, parent);
        assert_eq!(oversized, parent);
    }

    #[test]
    fn columns_split_evenly() {
        let [left, right] = columns::<2>(Rect::new(0, 0, 100, 10));
        assert_eq!(left.width, 50);
        assert_eq!(right.width, 50);
        let [a, b, c] = columns::<3>(Rect::new(0, 0, 90, 10));
        assert_eq!((a.width, b.width, c.width), (30, 30, 30));
    }

    #[test]
    fn table_offsets_follow_the_selection() {
        use super::table_offset;
        // Everything fits: never scroll.
        assert_eq!(table_offset(Some(5), 6, 10, 0), 0);
        assert_eq!(table_offset(None, 100, 0, 7), 0);
        // Selection inside the first page: no scroll.
        assert_eq!(table_offset(Some(3), 30, 10, 0), 0);
        // Moving below the viewport: selection lands on the last visible row.
        assert_eq!(table_offset(Some(15), 30, 10, 0), 6);
        // At the end: offset capped so the last page is full.
        assert_eq!(table_offset(Some(29), 30, 10, 6), 20);
        assert_eq!(table_offset(Some(99), 30, 10, 0), 20, "selection clamped");
        assert_eq!(table_offset(None, 30, 10, 0), 0);
        // Moving above the viewport: selection lands on the first visible row.
        assert_eq!(table_offset(Some(4), 30, 10, 20), 4);
        // A shrunken list pulls a stale offset back to a full last page.
        assert_eq!(table_offset(Some(12), 15, 10, 20), 5);
    }

    /// Scrolling back up from the bottom moves the selection to the top of the
    /// viewport before the list itself scrolls.
    #[test]
    fn table_offsets_stay_put_while_the_selection_is_visible() {
        use super::table_offset;
        let (count, height) = (30, 10);
        let mut offset = table_offset(Some(29), count, height, 0);
        assert_eq!(offset, 20);
        for selected in (20..29).rev() {
            offset = table_offset(Some(selected), count, height, offset);
            assert_eq!(offset, 20, "selection {selected} is still on screen");
        }
        offset = table_offset(Some(19), count, height, offset);
        assert_eq!(offset, 19);
    }

    #[test]
    fn the_detail_pane_goes_beside_below_or_away() {
        use super::{DETAIL_MIN_WIDTH, STACKED_DETAIL_HEIGHT, split_beside};
        let (table, detail) = split_beside(Rect::new(0, 0, 160, 20), 70);
        assert_eq!((table.width, detail.unwrap().x), (70, 70));
        let (table, _) = split_beside(Rect::new(0, 0, 120, 20), 100);
        assert_eq!(
            table.width,
            120 - DETAIL_MIN_WIDTH,
            "the pane keeps its minimum"
        );
        let (table, detail) = split_beside(Rect::new(0, 0, 90, 30), 70);
        assert_eq!(table.width, 90);
        assert_eq!(detail.unwrap().y, 30 - STACKED_DETAIL_HEIGHT);
        let (table, detail) = split_beside(Rect::new(0, 0, 90, 20), 70);
        assert_eq!((table.width, detail), (90, None));
    }

    #[test]
    fn bars_fill_proportionally() {
        assert_eq!(bar(0.0, 4), "····");
        assert_eq!(bar(0.5, 4), "██··");
        assert_eq!(bar(1.0, 4), "████");
        assert_eq!(bar(7.0, 2), "██");
    }
}
