//! The welcome animation that `fda` plays on its first run in a terminal.
//!
//! The animation has four parts:
//!
//! 1. The intro. The window draws itself.
//! 2. The film. The Feldera bird flies at the camera, makes a side-on pass and
//!    lands on the wordmark. A Violet band reveals the wordmark.
//! 3. The tagline. The tagline appears one character at a time.
//! 4. The final wingbeat. The landed bird beats its wings one time.
//!
//! The picture is made of braille characters. Each character cell holds a 2x4
//! grid of dots.
//!
//! `first_run` plays the animation one time per computer. `play` plays it on
//! each call, for `fda welcome`. Any key skips to the final frame.

mod color;
mod film;
mod reel;
mod term;
mod window;

use std::io::{IsTerminal, StdoutLock, Write, stdout};
use std::time::{Duration, Instant};

use color::{ColorMode, Theme};
use film::Film;
use reel::Reel;
use term::{Cursor, Keys, Stop};
use window::{TAGLINE, Window};

/// A point as [x, y].
type Pt = [f64; 2];

const FPS: f64 = 30.0;
/// The duration of the intro.
const INTRO_SECS: f64 = 1.1;
/// The number of tagline characters that appear per second.
const TYPING_CPS: f64 = 45.0;
/// The duration of the final wingbeat.
const FLAP_SECS: f64 = 1.8;

/// The smoothstep function. Returns 0 if `x` is at or below `e0` and 1 if `x`
/// is at or above `e1`. Between them, the result rises on a smooth curve.
fn smooth(e0: f64, e1: f64, x: f64) -> f64 {
    let x = ((x - e0) / (e1 - e0)).clamp(0.0, 1.0);
    x * x * (3.0 - 2.0 * x)
}

/// Returns the value of the environment variable `name`, or `None` if it is not set.
fn env_var(name: &str) -> Option<String> {
    std::env::var_os(name).map(|v| v.to_string_lossy().into_owned())
}

/// Returns the width between the side borders of the window for a terminal of `cols`
/// columns. At the lower limit of 60 columns, the panel text fits without a cut. The window
/// does not grow above 100 columns.
fn inner_width(cols: usize) -> usize {
    cols.saturating_sub(4).clamp(60, 100)
}

/// Returns true if the terminal can hold the window and one more row for the cursor. In a
/// shorter terminal, a cursor move up stops at the top of the screen, and the frames go to
/// the wrong rows.
fn fits(cols: usize, rows: usize) -> bool {
    let inner = inner_width(cols);
    cols >= inner + 4 && rows > Window::FILM_TOP + Film::rows(inner) + Window::BELOW_FILM
}

/// Returns true if the animation can play. stdin must be a terminal because a key press
/// skips the animation.
fn animation_allowed(
    stdout_tty: bool,
    stdin_tty: bool,
    env: impl Fn(&str) -> Option<String>,
) -> bool {
    stdout_tty
        && stdin_tty
        && env("CI").is_none()
        && env("FDA_NO_ANIMATION").is_none()
        && env("TERM").is_none_or(|t| t != "dumb")
}

fn can_animate() -> bool {
    animation_allowed(
        stdout().is_terminal(),
        std::io::stdin().is_terminal(),
        env_var,
    )
}

/// Writes the first-run marker file in `dir`. Returns true only if this call
/// wrote the marker. Returns false if the marker exists or if the write fails.
fn claim_first_run(dir: &std::path::Path) -> bool {
    let marker = dir.join("welcome-shown");
    !marker.exists() && std::fs::create_dir_all(dir).is_ok() && std::fs::write(&marker, b"").is_ok()
}

/// Sleeps until the end of frame `i` of a part that started at `start`. One slow frame does
/// not delay the frames after it.
fn pace(start: Instant, i: usize) {
    let due = start + Duration::from_secs_f64((i + 1) as f64 / FPS);
    std::thread::sleep(due.saturating_duration_since(Instant::now()));
}

/// Plays the four parts of the animation. Returns how the viewer stopped the
/// animation, or `None` if it played to the end.
///
/// `animate` prints the window one time and then overwrites its rows in place.
/// Between writes, the cursor is on the row below the window.
///
/// | Escape sequence | Effect |
/// |---|---|
/// | `ESC [ n F` | Moves the cursor up n rows, to the first column |
/// | `ESC [ n E` | Moves the cursor down n rows, to the first column |
/// | `ESC [ n C` | Moves the cursor right n columns |
/// | `ESC [ 2 K` | Erases the row |
fn animate(film: &Film, window: &Window, keys: &Keys, out: &mut StdoutLock) -> Option<Stop> {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(5, |d| d.as_nanos() as u64);
    let mut reel = Reel::new(seed);
    let total = Window::FILM_TOP + film.h + Window::BELOW_FILM;
    // Overwrites the frame rows of the window with `frame`.
    let draw = |out: &mut StdoutLock, frame: &[String]| {
        let rows: Vec<String> = frame.iter().map(|l| window.wrap(l)).collect();
        let _ = write!(
            out,
            "\x1b[{}F\x1b[2K{}\n\x1b[{}E",
            film.h + Window::BELOW_FILM,
            rows.join("\n\x1b[2K"),
            Window::BELOW_FILM,
        );
        let _ = out.flush();
    };

    // Part 1, the intro. Print `total` empty rows first, so that the terminal scrolls before
    // the first cursor move up.
    let _ = write!(out, "{}", "\n".repeat(total));
    let mut stop = None;
    let frames = (INTRO_SECS * FPS) as usize;
    let start = Instant::now();
    for i in 0..=frames {
        stop = keys.stop();
        if stop.is_some() {
            break;
        }
        let rows = window.intro_rows(film.h, i as f64 / frames as f64);
        let _ = writeln!(out, "\x1b[{total}F\x1b[2K{}", rows.join("\n\x1b[2K"));
        let _ = out.flush();
        pace(start, i);
    }
    let _ = write!(out, "\x1b[{total}F");
    for row in window.rows(&vec![" ".repeat(window.inner); film.h], false) {
        let _ = writeln!(out, "\x1b[2K{row}");
    }

    // Part 2, the film. A key press skips to the final frame.
    let start = Instant::now();
    for i in 0..Reel::frame_count() {
        stop = stop.or_else(|| keys.stop());
        if stop.is_some() {
            break;
        }
        draw(out, &reel.frame(film, i));
        pace(start, i);
    }
    if stop.is_some() {
        draw(out, &film.final_frame());
    }
    if stop == Some(Stop::Interrupt) {
        return stop;
    }

    // Part 3, the tagline. After a skip, all characters appear together. The tagline row is
    // `up` rows above the cursor.
    let up = Window::BELOW_FILM - 1;
    let _ = write!(out, "\x1b[{up}F\x1b[{}C", 1 + Window::TAGLINE_INDENT);
    let (band, reset) = (window.band(), window.color.reset());
    for (i, part) in TAGLINE.iter().enumerate() {
        let style = if i == 1 { band.as_str() } else { "" };
        for ch in part.chars() {
            let _ = write!(out, "{style}{ch}{reset}");
            if stop.is_none() {
                let _ = out.flush();
                std::thread::sleep(Duration::from_secs_f64(1.0 / TYPING_CPS));
            }
        }
    }
    let _ = write!(out, "\x1b[{up}E");
    if stop.is_some() {
        return stop;
    }

    // Part 4, the final wingbeat.
    let frames = (FLAP_SECS * FPS) as usize;
    let start = Instant::now();
    for i in 0..=frames {
        stop = keys.stop();
        if stop.is_some() {
            break;
        }
        draw(out, &reel.flap_frame(film, i as f64 / frames as f64));
        pace(start, i);
    }
    draw(out, &film.final_frame());
    stop
}

/// Plays the animation, or prints the window with the final frame if the animation cannot
/// play. `host` is the Feldera host, which the CONNECTION panel shows.
pub fn play(host: &str) {
    let size = term::size();
    // Assume 80 columns if the terminal size is unknown.
    let inner = inner_width(size.map_or(80, |(cols, _)| cols));
    let color = ColorMode::detect();
    let env_theme = env_var("COLORFGBG").and_then(|v| Theme::from_colorfgbg(&v));
    let mut film = Film::new(inner, color);
    let mut window = Window {
        inner,
        color,
        host: host.to_string(),
        muted: env_theme.unwrap_or(Theme::Dark).muted(),
    };
    let mut out = stdout().lock();
    if !can_animate() || !size.is_some_and(|(cols, rows)| fits(cols, rows)) {
        // The terminal scrolls plain rows, so this output is correct at any terminal size.
        for row in window.rows(&film.final_frame(), true) {
            let _ = writeln!(out, "{row}");
        }
        return;
    }

    // `keys` and `_cursor` restore the terminal at the end of this block. The
    // block ends before `exit`, because `exit` runs no destructors.
    let stop = {
        let keys = Keys::new();
        // If `COLORFGBG` is not set, ask the terminal for its background color.
        // Assume a dark background if the terminal does not answer.
        let theme = env_theme
            .or_else(|| keys.background(&mut out))
            .unwrap_or(Theme::Dark);
        film.muted = theme.muted();
        window.muted = theme.muted();
        let _cursor = Cursor::hide(&mut out);
        animate(&film, &window, &keys, &mut out)
    };
    let _ = out.flush();
    if stop == Some(Stop::Interrupt) {
        // 130 is the exit status of a process that Ctrl-C ends. It is 128 plus
        // the number of SIGINT.
        std::process::exit(130);
    }
}

/// Plays the animation if this is the first `fda` command on this computer that can show it.
/// `reads_stdin` is true for a command that reads stdin. The first typed key must reach that
/// command, so the animation does not play.
pub fn first_run(host: &str, reads_stdin: bool) {
    // Do not claim the first run in a terminal that is too small. A later run
    // in a larger terminal plays the animation.
    let fits_now = term::size().is_some_and(|(cols, rows)| fits(cols, rows));
    if reads_stdin || !can_animate() || !fits_now {
        return;
    }
    let Some(dirs) = directories::ProjectDirs::from("com", "Feldera", "fda") else {
        return;
    };
    // Claim the first run before the animation plays, so that an interrupted
    // animation does not play again.
    if claim_first_run(dirs.config_dir()) {
        play(host);
    }
}

/// Helpers that the tests in `splash.rs` and in `splash/` share.
#[cfg(test)]
mod test_util {
    /// Returns `line` without its escape sequences.
    pub fn strip(line: &str) -> String {
        let mut out = String::new();
        let mut esc = false;
        for ch in line.chars() {
            match (esc, ch) {
                (false, '\x1b') => esc = true,
                (true, c) if c.is_ascii_alphabetic() => esc = false,
                (true, _) => {}
                (false, c) => out.push(c),
            }
        }
        out
    }

    /// Returns the rows of `frame` without their escape sequences.
    pub fn plain(frame: &[String]) -> Vec<String> {
        frame.iter().map(|l| strip(l)).collect()
    }

    /// Returns a lookup function for an environment that has only the variables `vars`.
    pub fn fake_env(
        vars: &'static [(&'static str, &'static str)],
    ) -> impl Fn(&str) -> Option<String> + Copy {
        move |name: &str| {
            vars.iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| v.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_util::fake_env;
    use super::*;

    #[test]
    fn animation_needs_two_terminals_and_a_clear_environment() {
        assert!(animation_allowed(true, true, fake_env(&[])));
        assert!(animation_allowed(
            true,
            true,
            fake_env(&[("TERM", "xterm-256color")])
        ));
        assert!(!animation_allowed(true, true, fake_env(&[("CI", "true")])));
        assert!(!animation_allowed(
            true,
            true,
            fake_env(&[("FDA_NO_ANIMATION", "1")])
        ));
        assert!(!animation_allowed(
            true,
            true,
            fake_env(&[("TERM", "dumb")])
        ));
        assert!(
            !animation_allowed(false, true, fake_env(&[])),
            "stdout is a pipe"
        );
        assert!(
            !animation_allowed(true, false, fake_env(&[])),
            "stdin is a pipe"
        );
    }

    #[test]
    fn window_must_fit_the_terminal() {
        // The window is 30 rows at 80 columns, so a default 80x24 terminal is too short.
        let need = Window::FILM_TOP + Film::rows(inner_width(80)) + Window::BELOW_FILM;
        assert_eq!(need, 30);
        assert!(!fits(80, 24));
        assert!(!fits(80, need), "no row left for the cursor");
        assert!(fits(80, need + 1));
        assert!(fits(80, 40));
        assert!(!fits(63, 60), "too narrow");
        assert!(fits(64, 60));
    }

    #[test]
    fn first_run_marker_is_claimed_once() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("fda");
        assert!(claim_first_run(&config), "first run plays");
        assert!(config.join("welcome-shown").exists());
        assert!(!claim_first_run(&config), "second run does not");

        // `blocked` is a file, so the config directory `blocked/fda` cannot be created.
        let blocked = dir.path().join("blocked");
        std::fs::write(&blocked, b"").unwrap();
        assert!(!claim_first_run(&blocked.join("fda")));
    }
}
