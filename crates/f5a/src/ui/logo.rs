//! The F5A wordmark for the header.

/// Six-row block wordmark, rendered with one gradient color per row.
pub const LOGO: [&str; 6] = [
    "███████╗███████╗ █████╗ ",
    "██╔════╝██╔════╝██╔══██╗",
    "█████╗  ███████╗███████║",
    "██╔══╝  ╚════██║██╔══██║",
    "██║     ███████║██║  ██║",
    "╚═╝     ╚══════╝╚═╝  ╚═╝",
];

/// Compact three-row mark for narrow terminals.
pub const LOGO_SMALL: [&str; 3] = ["▛▀▘ ▛▀▀ ▞▀▚", "▛▀  ▀▀▚ ▙▄▟", "▘   ▀▀▘ ▘ ▝"];

/// Character width of the large logo.
pub fn logo_width() -> u16 {
    LOGO[0].chars().count() as u16
}

/// Character width of the small logo.
pub fn logo_small_width() -> u16 {
    LOGO_SMALL[0].chars().count() as u16
}

#[cfg(test)]
mod tests {
    use super::{LOGO, LOGO_SMALL, logo_small_width, logo_width};

    #[test]
    fn logo_rows_share_one_width() {
        for row in LOGO {
            assert_eq!(row.chars().count() as u16, logo_width());
        }
        for row in LOGO_SMALL {
            assert_eq!(row.chars().count() as u16, logo_small_width());
        }
    }

    #[test]
    fn logo_fits_the_reserved_header_columns() {
        assert!(logo_width() <= 60);
        assert!(logo_small_width() <= 26);
    }
}
