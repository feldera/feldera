//! The console's palette: neon accents on a deep navy ground.

use ratatui::style::{Color, Modifier, Style};

use crate::app::capture::{CapturePhase, animation_frame};
use crate::model::pipeline::Health;

pub const BG: Color = Color::Rgb(9, 13, 24);
pub const PANEL: Color = Color::Rgb(15, 21, 36);
pub const SELECTION: Color = Color::Rgb(36, 52, 84);
pub const TEXT: Color = Color::Rgb(222, 232, 245);
pub const MUTED: Color = Color::Rgb(102, 118, 148);
pub const CYAN: Color = Color::Rgb(0, 229, 255);
pub const MAGENTA: Color = Color::Rgb(255, 92, 222);
pub const GREEN: Color = Color::Rgb(80, 250, 123);
pub const YELLOW: Color = Color::Rgb(241, 250, 140);
pub const ORANGE: Color = Color::Rgb(255, 170, 70);
pub const RED: Color = Color::Rgb(255, 85, 119);
pub const BLUE: Color = Color::Rgb(98, 168, 255);

/// Vertical gradient used by the logo, top row first.
pub const LOGO_GRADIENT: [Color; 6] = [
    Color::Rgb(0, 229, 255),
    Color::Rgb(64, 205, 250),
    Color::Rgb(128, 180, 246),
    Color::Rgb(192, 140, 236),
    Color::Rgb(255, 105, 226),
    Color::Rgb(255, 92, 180),
];

/// Frames for the busy spinner.
pub const SPINNER: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

pub fn text() -> Style {
    Style::default().fg(TEXT)
}

pub fn muted() -> Style {
    Style::default().fg(MUTED)
}

pub fn accent(color: Color) -> Style {
    Style::default().fg(color).add_modifier(Modifier::BOLD)
}

/// How long a fresh save blinks before settling on green.
const SAVED_FLASH_MILLIS: i64 = 3_000;

/// Style of a capture badge at `now_millis`: a recording pulses
/// through the logo gradient, a download alternates blue and cyan, a fresh
/// save blinks green and yellow for a moment, a failure stays red.
pub fn capture_style(phase: &CapturePhase, now_millis: i64) -> Style {
    let frame = animation_frame(now_millis);
    match phase {
        CapturePhase::Recording { .. } => accent(LOGO_GRADIENT[frame % LOGO_GRADIENT.len()]),
        CapturePhase::Fetching { .. } => accent(if frame % 4 < 2 { BLUE } else { CYAN }),
        CapturePhase::Saved {
            finished_millis, ..
        } if now_millis - finished_millis < SAVED_FLASH_MILLIS => {
            accent(if frame.is_multiple_of(2) {
                GREEN
            } else {
                YELLOW
            })
        }
        CapturePhase::Saved { .. } => accent(GREEN),
        CapturePhase::Failed { .. } => accent(RED),
    }
}

/// Steady color of a capture's phase, for prose that must not blink: the
/// status line reads the same whether the badge in the table is mid-pulse.
pub fn capture_steady_style(phase: &CapturePhase) -> Style {
    match phase {
        CapturePhase::Recording { .. } => accent(CYAN),
        CapturePhase::Fetching { .. } => accent(BLUE),
        CapturePhase::Saved { .. } => accent(GREEN),
        CapturePhase::Failed { .. } => accent(RED),
    }
}

/// Style for a status label given its health classification.
pub fn health_style(health: Health) -> Style {
    match health {
        Health::Good => accent(GREEN),
        Health::Busy => Style::default().fg(YELLOW),
        Health::Idle => Style::default().fg(MUTED),
        Health::Bad => accent(RED),
    }
}

/// Heat ramp for SQL lines and cost bars, `0.0..=1.0`.
pub fn heat_color(heat: f64) -> Color {
    let heat = heat.clamp(0.0, 1.0);
    if heat == 0.0 {
        return MUTED;
    }
    // Dark amber to bright red.
    let red = 140.0 + 115.0 * heat;
    let green = 150.0 - 110.0 * heat;
    let blue = 60.0 - 40.0 * heat;
    Color::Rgb(red as u8, green as u8, blue as u8)
}

#[cfg(test)]
mod tests {
    use super::{MUTED, health_style, heat_color};
    use crate::model::pipeline::Health;
    use ratatui::style::Color;

    #[test]
    fn health_maps_to_distinct_styles() {
        let styles: Vec<_> = [Health::Good, Health::Busy, Health::Idle, Health::Bad]
            .into_iter()
            .map(health_style)
            .collect();
        for (index, style) in styles.iter().enumerate() {
            for other in &styles[index + 1..] {
                assert_ne!(style, other);
            }
        }
    }

    #[test]
    fn heat_ramps_from_muted_to_red() {
        assert_eq!(heat_color(0.0), MUTED);
        assert_eq!(heat_color(-1.0), MUTED);
        let Color::Rgb(cold_r, cold_g, _) = heat_color(0.1) else {
            panic!("heat colors are RGB");
        };
        let Color::Rgb(hot_r, hot_g, _) = heat_color(1.0) else {
            panic!("heat colors are RGB");
        };
        assert!(hot_r > cold_r);
        assert!(hot_g < cold_g);
        assert_eq!(heat_color(2.0), heat_color(1.0));
    }

    #[test]
    fn samply_badges_pulse_and_settle() {
        use super::{GREEN, YELLOW, capture_style};
        use crate::app::capture::CapturePhase;
        let recording = CapturePhase::Recording {
            started_millis: 0,
            duration_secs: 30,
            accepted: true,
        };
        assert_ne!(capture_style(&recording, 0), capture_style(&recording, 150));
        let fetching = CapturePhase::Fetching {
            poll_at_millis: None,
            deadline_millis: 1,
            transport_failures: 0,
        };
        assert_ne!(capture_style(&fetching, 0), capture_style(&fetching, 300));
        let saved = CapturePhase::Saved {
            path: "x".to_string(),
            finished_millis: 0,
        };
        assert_eq!(capture_style(&saved, 0).fg, Some(GREEN));
        assert_eq!(capture_style(&saved, 150).fg, Some(YELLOW));
        assert_eq!(
            capture_style(&saved, 3_150).fg,
            Some(GREEN),
            "the blink ends"
        );
    }

    #[test]
    fn steady_capture_styles_never_change_between_frames() {
        use super::capture_steady_style;
        use crate::app::capture::CapturePhase;
        let phases = [
            CapturePhase::Recording {
                started_millis: 0,
                duration_secs: 30,
                accepted: true,
            },
            CapturePhase::Fetching {
                poll_at_millis: None,
                deadline_millis: 1,
                transport_failures: 0,
            },
            CapturePhase::Saved {
                path: "x".to_string(),
                finished_millis: 0,
            },
            CapturePhase::Failed {
                error: "boom".to_string(),
                finished_millis: 0,
            },
        ];
        let styles: Vec<_> = phases.iter().map(capture_steady_style).collect();
        for (index, style) in styles.iter().enumerate() {
            for other in &styles[index + 1..] {
                assert_ne!(style, other, "each phase keeps its own color");
            }
        }
    }
}
