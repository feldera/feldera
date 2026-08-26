//! Deterministic formatting helpers for the renderer.

/// Cut a label to terminal width, leaving space for an ellipsis.
pub fn truncate(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    if max_chars == 0 {
        return String::new();
    }
    if max_chars == 1 {
        return "…".to_string();
    }
    let prefix: String = text.chars().take(max_chars - 1).collect();
    format!("{prefix}…")
}

/// Format a byte count with a compact IEC suffix.
pub fn human_bytes(bytes: i64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let negative = bytes.is_negative();
    let mut value = bytes.unsigned_abs() as f64;
    let mut unit_index = 0;
    while value >= 1024.0 && unit_index < UNITS.len() - 1 {
        value /= 1024.0;
        unit_index += 1;
    }
    let sign = if negative { "-" } else { "" };
    if unit_index == 0 {
        format!("{sign}{} {}", value as u64, UNITS[unit_index])
    } else {
        format!("{sign}{value:.1} {}", UNITS[unit_index])
    }
}

/// Format a large scalar without losing its order of magnitude.
pub fn compact_number(value: i64) -> String {
    const SUFFIXES: [(&str, f64); 4] = [("G", 1e9), ("M", 1e6), ("K", 1e3), ("", 1.0)];
    let negative = value.is_negative();
    let absolute = value.unsigned_abs() as f64;
    let sign = if negative { "-" } else { "" };
    for (suffix, divisor) in SUFFIXES {
        if absolute >= divisor {
            if suffix.is_empty() {
                return format!("{sign}{}", absolute as u64);
            }
            return format!("{sign}{:.1}{suffix}", absolute / divisor);
        }
    }
    "0".to_string()
}

/// Format a rate in records per second.
pub fn rate(records_per_sec: f64) -> String {
    if records_per_sec >= 1e6 {
        format!("{:.1}M/s", records_per_sec / 1e6)
    } else if records_per_sec >= 1e3 {
        format!("{:.1}K/s", records_per_sec / 1e3)
    } else if records_per_sec > 0.0 {
        format!("{records_per_sec:.0}/s")
    } else {
        "0/s".to_string()
    }
}

/// Format an uptime given in milliseconds as `1d 2h`, `3h 4m`, `5m 6s`, `7s`.
pub fn uptime(uptime_millis: i64) -> String {
    let total_seconds = uptime_millis.max(0) / 1_000;
    let days = total_seconds / 86_400;
    let hours = (total_seconds % 86_400) / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;
    if days > 0 {
        format!("{days}d {hours}h")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else if minutes > 0 {
        format!("{minutes}m {seconds}s")
    } else {
        format!("{seconds}s")
    }
}

/// Render numeric samples as a compact left-to-right sparkline.
pub fn sparkline(values: &[i64], width: usize) -> String {
    const BARS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    if width == 0 || values.is_empty() {
        return String::new();
    }
    let first = values.len().saturating_sub(width);
    let samples = &values[first..];
    let min = *samples.iter().min().expect("non-empty samples");
    let max = *samples.iter().max().expect("non-empty samples");
    let range = max.saturating_sub(min);
    samples
        .iter()
        .map(|value| {
            let index = if range == 0 {
                if max > 0 { BARS.len() / 2 } else { 0 }
            } else {
                ((value.saturating_sub(min) as u128 * (BARS.len() - 1) as u128) / range as u128)
                    as usize
            };
            BARS[index]
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{compact_number, human_bytes, rate, sparkline, truncate, uptime};

    #[test]
    fn truncation_respects_character_boundaries() {
        assert_eq!(truncate("Feldera", 4), "Fel…");
        assert_eq!(truncate("张伟", 1), "…");
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("x", 0), "");
    }

    #[test]
    fn human_bytes_covers_sign_and_units() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1536), "1.5 KiB");
        assert_eq!(human_bytes(-1024), "-1.0 KiB");
        assert_eq!(human_bytes(3 * 1024 * 1024 * 1024), "3.0 GiB");
    }

    #[test]
    fn compact_numbers_keep_the_scale_visible() {
        assert_eq!(compact_number(999), "999");
        assert_eq!(compact_number(1_500), "1.5K");
        assert_eq!(compact_number(-2_000_000), "-2.0M");
        assert_eq!(compact_number(0), "0");
        assert_eq!(compact_number(3_000_000_000), "3.0G");
    }

    #[test]
    fn rates_scale_with_magnitude() {
        assert_eq!(rate(0.0), "0/s");
        assert_eq!(rate(950.0), "950/s");
        assert_eq!(rate(1_500.0), "1.5K/s");
        assert_eq!(rate(2_500_000.0), "2.5M/s");
    }

    #[test]
    fn uptime_picks_the_two_largest_units() {
        assert_eq!(uptime(5_000), "5s");
        assert_eq!(uptime(65_000), "1m 5s");
        assert_eq!(uptime(3_700_000), "1h 1m");
        assert_eq!(uptime(90_000_000), "1d 1h");
        assert_eq!(uptime(-5), "0s");
    }

    #[test]
    fn sparkline_has_one_cell_per_visible_sample() {
        assert_eq!(sparkline(&[], 8), "");
        assert_eq!(sparkline(&[3, 3, 3], 8), "▅▅▅");
        assert_eq!(sparkline(&[0, 0], 8), "▁▁");
        assert_eq!(sparkline(&[1, 2, 3, 4], 2), "▁█");
    }
}
