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

/// First visible row of a table viewport: scrolls just enough to keep the
/// selection on screen, pinning it to the last row once past the first page.
pub fn table_offset(selected: Option<usize>, count: usize, height: usize) -> usize {
    if height == 0 || count <= height {
        return 0;
    }
    let selected = selected.unwrap_or(0).min(count.saturating_sub(1));
    selected
        .saturating_sub(height.saturating_sub(1))
        .min(count - height)
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
        assert_eq!(table_offset(Some(5), 6, 10), 0);
        assert_eq!(table_offset(None, 100, 0), 0);
        // Selection inside the first page: no scroll.
        assert_eq!(table_offset(Some(3), 30, 10), 0);
        // Past the first page: selection pinned to the last visible row.
        assert_eq!(table_offset(Some(15), 30, 10), 6);
        // At the end: offset capped so the last page is full.
        assert_eq!(table_offset(Some(29), 30, 10), 20);
        assert_eq!(table_offset(Some(99), 30, 10), 20, "selection clamped");
        assert_eq!(table_offset(None, 30, 10), 0);
    }

    #[test]
    fn bars_fill_proportionally() {
        assert_eq!(bar(0.0, 4), "····");
        assert_eq!(bar(0.5, 4), "██··");
        assert_eq!(bar(1.0, 4), "████");
        assert_eq!(bar(7.0, 2), "██");
    }
}
