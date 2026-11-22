//! Access to the terminal. This file reads the size of the terminal, hides its
//! cursor and reads the keys that the viewer presses while the animation plays.

use std::io::{Write, stdout};
use std::time::{Duration, Instant};

use super::color::Theme;

pub(super) use sys::{Keys, size};

#[cfg(unix)]
mod sys {
    use std::sync::OnceLock;

    /// Returns the size of the terminal as (columns, rows), or `None` if the
    /// terminal does not report its size.
    pub fn size() -> Option<(usize, usize)> {
        // SAFETY: `winsize` is a plain C struct of integers, so all zeroes is a valid value.
        let mut ws: libc::winsize = unsafe { std::mem::zeroed() };
        // SAFETY: TIOCGWINSZ writes one `winsize` through the pointer. The pointer
        // points to `ws`, which is live and writable for the whole call.
        let ok = unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws) } == 0;
        (ok && ws.ws_col > 0 && ws.ws_row > 0).then_some((ws.ws_col as usize, ws.ws_row as usize))
    }

    /// The terminal settings from before the animation. The signal handler
    /// restores them.
    static SAVED_TERMIOS: OnceLock<libc::termios> = OnceLock::new();

    /// The signals on which `restore_and_reraise` restores the terminal. A
    /// process cannot catch SIGKILL, so SIGKILL leaves the terminal in raw mode.
    const RESTORE_ON: [libc::c_int; 4] = [libc::SIGTERM, libc::SIGHUP, libc::SIGQUIT, libc::SIGINT];

    /// The signal handler. Restores the terminal settings and shows the cursor.
    /// Then sends the signal again with its default action, which ends `fda`.
    extern "C" fn restore_and_reraise(sig: libc::c_int) {
        // Resets the text style, shows the cursor and starts a new line.
        const SHOW: &[u8] = b"\x1b[0m\x1b[?25h\r\n";
        // SAFETY: tcsetattr, write, signal and raise are async-signal-safe.
        // `Keys::new` sets `SAVED_TERMIOS` before it installs this handler, so
        // the termios pointer is valid. SHOW is a static buffer of SHOW.len() bytes.
        unsafe {
            if let Some(t) = SAVED_TERMIOS.get() {
                libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, t);
            }
            libc::write(libc::STDOUT_FILENO, SHOW.as_ptr().cast(), SHOW.len());
            libc::signal(sig, libc::SIG_DFL);
            libc::raise(sig);
        }
    }

    /// While a `Keys` exists, stdin is in raw mode. The terminal does not buffer lines, echo
    /// keys or change Ctrl-C into a signal, so each key arrives as a byte. `Keys` restores the
    /// terminal settings when it drops, and on each signal in `RESTORE_ON`.
    pub struct Keys {
        /// The settings to restore. `None` if `new` did not change the settings
        /// of stdin.
        saved: Option<libc::termios>,
        /// The signal handlers from before `new`.
        old_handlers: Vec<(libc::c_int, libc::sighandler_t)>,
    }

    impl Keys {
        /// Changes stdin to raw mode and installs the signal handlers. Changes
        /// nothing if stdin is not a terminal.
        pub fn new() -> Self {
            let off = Keys {
                saved: None,
                old_handlers: Vec::new(),
            };
            // SAFETY: `termios` is a plain C struct of integers, so all zeroes is a valid value.
            let mut t: libc::termios = unsafe { std::mem::zeroed() };
            // SAFETY: tcgetattr writes one `termios` through the pointer, which points to `t`.
            if unsafe { libc::tcgetattr(libc::STDIN_FILENO, &mut t) } != 0 {
                return off;
            }
            let saved = t;
            // `play` runs one time per process, so no other call sets this value.
            if SAVED_TERMIOS.set(saved).is_err() {
                return off;
            }
            // Install the handlers before the change to raw mode, so that a
            // signal cannot end `fda` while the terminal is in raw mode.
            let handler = restore_and_reraise as extern "C" fn(libc::c_int) as libc::sighandler_t;
            let old_handlers = RESTORE_ON
                .iter()
                // SAFETY: installs a handler that makes only async-signal-safe calls.
                .map(|&sig| (sig, unsafe { libc::signal(sig, handler) }))
                .collect();
            t.c_lflag &= !(libc::ICANON | libc::ECHO | libc::ISIG);
            // With VMIN = 0 and VTIME = 0, `read` returns immediately if no byte waits.
            t.c_cc[libc::VMIN] = 0;
            t.c_cc[libc::VTIME] = 0;
            // SAFETY: tcsetattr reads one `termios` through the pointer, which points to `t`.
            unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &t) };
            Keys {
                saved: Some(saved),
                old_handlers,
            }
        }

        /// Returns true if stdin is in raw mode. If it is not, `pressed`
        /// always returns `None`.
        pub fn is_raw(&self) -> bool {
            self.saved.is_some()
        }

        /// Returns the bytes that wait on stdin, or `None` if no byte waits.
        /// Does not block.
        pub fn pressed(&self) -> Option<Vec<u8>> {
            self.saved?;
            let mut buf = [0u8; 64];
            // SAFETY: `buf` is a live, writable buffer of `buf.len()` bytes, and
            // `read` writes at most `buf.len()` bytes to it.
            let n = unsafe { libc::read(libc::STDIN_FILENO, buf.as_mut_ptr().cast(), buf.len()) };
            (n > 0).then(|| buf[..n as usize].to_vec())
        }
    }

    impl Drop for Keys {
        fn drop(&mut self) {
            if let Some(t) = self.saved {
                // SAFETY: tcsetattr reads one `termios` through the pointer, which
                // points to `t`, the settings that `new` saved.
                unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &t) };
                for &(sig, old) in &self.old_handlers {
                    // SAFETY: restores the handler from before `new`.
                    unsafe { libc::signal(sig, old) };
                }
            }
        }
    }
}

/// `fda` does not read the terminal on other platforms. `size` returns `None`
/// there, so the animation does not play.
#[cfg(not(unix))]
mod sys {
    pub fn size() -> Option<(usize, usize)> {
        None
    }

    pub struct Keys;

    impl Keys {
        pub fn new() -> Self {
            Keys
        }

        pub fn is_raw(&self) -> bool {
            false
        }

        pub fn pressed(&self) -> Option<Vec<u8>> {
            None
        }
    }
}

/// How the viewer stopped the animation.
#[derive(Clone, Copy, PartialEq)]
pub(super) enum Stop {
    /// A key press. The animation skips to the final frame.
    Skip,
    /// Ctrl-C. The animation skips to the final frame, then `fda` exits.
    Interrupt,
}

impl Keys {
    /// Returns how the viewer stopped the animation since the last call, or
    /// `None` if the viewer pressed no key.
    pub(super) fn stop(&self) -> Option<Stop> {
        let bytes = self.pressed()?;
        // In raw mode, Ctrl-C arrives as the byte 0x03.
        Some(if bytes.contains(&0x03) {
            Stop::Interrupt
        } else {
            Stop::Skip
        })
    }

    /// Asks the terminal for its background color with an OSC 11 query. Waits up to 150 ms,
    /// because a terminal that does not support OSC 11 does not answer.
    pub(super) fn background(&self, out: &mut impl Write) -> Option<Theme> {
        if !self.is_raw() {
            // Without raw mode, the terminal echoes the reply on the screen.
            return None;
        }
        let _ = write!(out, "\x1b]11;?\x1b\\");
        let _ = out.flush();
        let deadline = Instant::now() + Duration::from_millis(150);
        let mut reply = Vec::new();
        while Instant::now() < deadline {
            match self.pressed() {
                Some(bytes) => {
                    reply.extend(bytes);
                    if let Some(theme) = Theme::from_osc11(&reply) {
                        return Some(theme);
                    }
                }
                None => std::thread::sleep(Duration::from_millis(5)),
            }
        }
        None
    }
}

/// Hides the cursor while it exists. Resets the text style and shows the
/// cursor when it drops.
pub(super) struct Cursor;

impl Cursor {
    pub(super) fn hide(out: &mut impl Write) -> Self {
        let _ = write!(out, "\x1b[?25l");
        Cursor
    }
}

impl Drop for Cursor {
    fn drop(&mut self) {
        let mut out = stdout().lock();
        let _ = write!(out, "\x1b[0m\x1b[?25h");
        let _ = out.flush();
    }
}
