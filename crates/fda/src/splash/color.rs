//! The colors of the animation. This file has the brand palette, detects the
//! color support of the terminal, and detects if its background is dark or light.

use std::io::{IsTerminal, stdout};

use super::env_var;

pub(super) type Rgb = (u8, u8, u8);

// The Feldera brand palette. The landed logo has the default text color of the
// terminal, because that color contrasts with a dark and with a light background.
pub(super) const VIOLET: Rgb = (0x62, 0x4E, 0xB3);
pub(super) const STEEL: Rgb = (0x99, 0xA2, 0xB3);
pub(super) const LICHEN: Rgb = (0x96, 0x96, 0x64);
pub(super) const CLAY: Rgb = (0xAE, 0xA6, 0x8C);
pub(super) const STONE: Rgb = (0xD7, 0xD7, 0xCC);
pub(super) const LICHEN_TINT: Rgb = (0xDD, 0xDD, 0xC1);
const LICHEN_SHADE: Rgb = (0x63, 0x63, 0x3D);
pub(super) const INK: Rgb = (0x20, 0x1F, 0x1E);

/// The color support of the terminal.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum ColorMode {
    Off,
    Ansi256,
    TrueColor,
}

impl ColorMode {
    pub(super) fn detect() -> Self {
        Self::from_env(stdout().is_terminal(), env_var)
    }

    /// Returns the color support. `tty` is true if stdout is a terminal, and `env` returns the
    /// value of an environment variable.
    fn from_env(tty: bool, env: impl Fn(&str) -> Option<String>) -> Self {
        if !tty || env("NO_COLOR").is_some() || env("TERM").as_deref() == Some("dumb") {
            ColorMode::Off
        } else if matches!(env("COLORTERM").as_deref(), Some("truecolor" | "24bit")) {
            ColorMode::TrueColor
        } else {
            ColorMode::Ansi256
        }
    }

    pub(super) fn fg(self, c: Rgb) -> String {
        self.sgr(38, c)
    }

    pub(super) fn bg(self, c: Rgb) -> String {
        self.sgr(48, c)
    }

    /// Returns the SGR escape sequence for the color `c`. `layer` is 38 for
    /// the text color and 48 for the background color.
    fn sgr(self, layer: u8, (r, g, b): Rgb) -> String {
        match self {
            ColorMode::Off => String::new(),
            ColorMode::TrueColor => format!("\x1b[{layer};2;{r};{g};{b}m"),
            ColorMode::Ansi256 => format!("\x1b[{layer};5;{}m", ansi256((r, g, b))),
        }
    }

    pub(super) fn reset(self) -> &'static str {
        if self == ColorMode::Off {
            ""
        } else {
            "\x1b[0m"
        }
    }
}

/// Returns the index of the xterm 256-color palette entry that is nearest to `c`. The
/// candidates are the nearest entry of the color cube and the nearest entry of the gray ramp.
fn ansi256(c: Rgb) -> u8 {
    // The six levels of a channel in the color cube. The steps between them
    // are not equal.
    const LEVELS: [u8; 6] = [0, 95, 135, 175, 215, 255];
    let near = |v: u8| {
        (0..6)
            .min_by_key(|&i| (LEVELS[i] as i32 - v as i32).abs())
            .unwrap()
    };
    let dist = |a: Rgb, b: Rgb| {
        let d = |x: u8, y: u8| (x as i32 - y as i32).pow(2);
        d(a.0, b.0) + d(a.1, b.1) + d(a.2, b.2)
    };
    let (ri, gi, bi) = (near(c.0), near(c.1), near(c.2));
    let cube = (LEVELS[ri], LEVELS[gi], LEVELS[bi]);
    // The gray ramp has 24 entries, at the indexes 232 to 255. Entry `k` has
    // the value 8 + 10 * k in each channel.
    let avg = (c.0 as i32 + c.1 as i32 + c.2 as i32) / 3;
    let k = ((avg - 8 + 5) / 10).clamp(0, 23) as u8;
    let g = 8 + 10 * k;
    if dist(c, (g, g, g)) < dist(c, cube) {
        232 + k
    } else {
        (16 + 36 * ri + 6 * gi + bi) as u8
    }
}

/// The background of the terminal. Secondary text needs a different color for
/// each background, because no palette color has enough contrast on both.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Theme {
    Dark,
    Light,
}

impl Theme {
    /// Returns the color for secondary text, such as labels, hints and descriptions.
    pub(super) fn muted(self) -> Rgb {
        match self {
            Theme::Dark => STEEL,
            Theme::Light => LICHEN_SHADE,
        }
    }

    /// Returns the theme from the value of `COLORFGBG`, which some terminals set. Its last
    /// field is the background as an ANSI color number, where 7 and 15 are white.
    pub(super) fn from_colorfgbg(v: &str) -> Option<Theme> {
        let bg: u8 = v.rsplit(';').next()?.trim().parse().ok()?;
        match bg {
            7 | 15 => Some(Theme::Light),
            0..=15 => Some(Theme::Dark),
            _ => None,
        }
    }

    /// Returns the theme from the reply to an OSC 11 query, or `None` if the reply is not
    /// complete. The reply has the form `ESC ] 11 ; rgb:RRRR/GGGG/BBBB`, with 1 to 4 hex digits
    /// per channel.
    pub(super) fn from_osc11(reply: &[u8]) -> Option<Theme> {
        let text = std::str::from_utf8(reply).ok()?;
        let rgb = &text[text.find("rgb:")? + 4..];
        let end = rgb.find(['\x07', '\x1b']).unwrap_or(rgb.len());
        let parts: Vec<f64> = rgb[..end]
            .split('/')
            .map(|h| {
                // The maximum value of a channel with n hex digits is 16^n - 1.
                let max = 16f64.powi(h.len() as i32) - 1.0;
                u32::from_str_radix(h, 16).ok().map(|v| v as f64 / max)
            })
            .collect::<Option<_>>()?;
        let [r, g, b] = parts[..] else { return None };
        // The Rec. 709 weights for the brightness of each channel.
        let luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b;
        Some(if luminance > 0.5 {
            Theme::Light
        } else {
            Theme::Dark
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::splash::test_util::fake_env;

    #[test]
    fn color_needs_a_terminal_that_draws_it() {
        let truecolor = fake_env(&[("COLORTERM", "truecolor")]);
        assert_eq!(ColorMode::from_env(true, truecolor), ColorMode::TrueColor);
        assert_eq!(ColorMode::from_env(true, fake_env(&[])), ColorMode::Ansi256);
        // If stdout is a pipe or a file, the result is `Off` even with `COLORTERM` set.
        assert_eq!(ColorMode::from_env(false, truecolor), ColorMode::Off);
        let no_color = fake_env(&[("COLORTERM", "truecolor"), ("NO_COLOR", "1")]);
        assert_eq!(ColorMode::from_env(true, no_color), ColorMode::Off);
        let dumb = fake_env(&[("COLORTERM", "truecolor"), ("TERM", "dumb")]);
        assert_eq!(ColorMode::from_env(true, dumb), ColorMode::Off);
    }

    /// Returns the WCAG contrast ratio between two sRGB colors.
    fn contrast(a: Rgb, b: Rgb) -> f64 {
        let lum = |(r, g, b): Rgb| {
            let ch = |v: u8| {
                let c = v as f64 / 255.0;
                if c <= 0.03928 {
                    c / 12.92
                } else {
                    ((c + 0.055) / 1.055).powf(2.4)
                }
            };
            0.2126 * ch(r) + 0.7152 * ch(g) + 0.0722 * ch(b)
        };
        let (x, y) = (lum(a), lum(b));
        (x.max(y) + 0.05) / (x.min(y) + 0.05)
    }

    /// Returns the color of the xterm 256-color palette entry `i`.
    fn xterm_rgb(i: u8) -> Rgb {
        const LEVELS: [u8; 6] = [0, 95, 135, 175, 215, 255];
        match i {
            16..=231 => {
                let i = i - 16;
                (
                    LEVELS[(i / 36) as usize],
                    LEVELS[(i / 6 % 6) as usize],
                    LEVELS[(i % 6) as usize],
                )
            }
            232..=255 => {
                let g = 8 + 10 * (i - 232);
                (g, g, g)
            }
            _ => panic!("not a cube or gray index: {i}"),
        }
    }

    #[test]
    fn brand_colors_survive_256_color_terminals() {
        let dist = |a: Rgb, b: Rgb| {
            let d = |x: u8, y: u8| (x as i32 - y as i32).pow(2);
            d(a.0, b.0) + d(a.1, b.1) + d(a.2, b.2)
        };
        let brand = [
            VIOLET,
            STEEL,
            LICHEN,
            CLAY,
            STONE,
            LICHEN_TINT,
            LICHEN_SHADE,
            INK,
        ];
        for c in brand {
            // `ansi256` returns the nearest of the 240 cube and gray entries.
            let best = (16..=255u8).map(|i| dist(c, xterm_rgb(i))).min().unwrap();
            assert_eq!(
                dist(c, xterm_rgb(ansi256(c))),
                best,
                "{c:?} is not mapped to its nearest entry"
            );
        }
        let tint = xterm_rgb(ansi256(LICHEN_TINT));
        assert!(tint.2 < tint.0, "Lichen tint turned neutral gray: {tint:?}");
        let violet = xterm_rgb(ansi256(VIOLET));
        assert!(
            violet.2 > violet.0 + 40,
            "Violet turned lavender or gray: {violet:?}"
        );
        // Ink text on the Lichen tint band has a contrast of 7 or more, the WCAG AAA level.
        let band = contrast(xterm_rgb(ansi256(INK)), tint);
        assert!(band >= 7.0, "tagline band contrast {band:.1}");
    }

    #[test]
    fn secondary_text_reads_on_both_themes() {
        // The light and the dark background of the brand.
        const PAPER: Rgb = (0xFA, 0xF9, 0xF6);
        const ONYX: Rgb = (0x0F, 0x10, 0x0E);
        let light = contrast(Theme::Light.muted(), PAPER);
        let dark = contrast(Theme::Dark.muted(), ONYX);
        // 4.5 is the WCAG AA contrast for normal text.
        assert!(light >= 4.5, "light theme: {light:.1}");
        assert!(dark >= 4.5, "dark theme: {dark:.1}");
    }

    #[test]
    fn terminal_theme_is_read_from_the_terminal() {
        assert_eq!(Theme::from_colorfgbg("15;0"), Some(Theme::Dark));
        assert_eq!(Theme::from_colorfgbg("0;15"), Some(Theme::Light));
        assert_eq!(Theme::from_colorfgbg("0;default;7"), Some(Theme::Light));
        assert_eq!(Theme::from_colorfgbg("nonsense"), None);
        let white = b"\x1b]11;rgb:ffff/ffff/ffff\x1b\\";
        let onyx = b"\x1b]11;rgb:0f0f/1010/0e0e\x07";
        assert_eq!(Theme::from_osc11(white), Some(Theme::Light));
        assert_eq!(Theme::from_osc11(onyx), Some(Theme::Dark));
        assert_eq!(Theme::from_osc11(b"\x1b]11;rgb:ff"), None, "cut-off reply");
        assert_eq!(Theme::from_osc11(b"x"), None);
    }
}
