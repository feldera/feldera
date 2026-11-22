//! Access to the terminal. This file reads the size of the terminal, hides its
//! cursor and reads the keys that the viewer presses while the animation plays.

use std::io::{Write, stdout};
use std::time::{Duration, Instant};

use super::color::Theme;

use sys::RawInput;
pub(super) use sys::size;

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
        // `RawInput::new` sets `SAVED_TERMIOS` before it installs this handler, so
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

    /// While a `RawInput` exists, stdin is in raw mode. The terminal does not buffer lines,
    /// echo keys or change Ctrl-C into a signal, so each key arrives as a byte. `RawInput`
    /// restores the terminal settings when it drops, and on each signal in `RESTORE_ON`.
    pub struct RawInput {
        /// The settings to restore. `None` if `new` did not change the settings
        /// of stdin.
        saved: Option<libc::termios>,
        /// The signal handlers from before `new`.
        old_handlers: Vec<(libc::c_int, libc::sighandler_t)>,
    }

    impl RawInput {
        /// Changes stdin to raw mode and installs the signal handlers. Changes
        /// nothing if stdin is not a terminal.
        pub fn new() -> Self {
            let off = RawInput {
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
            RawInput {
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

    impl Drop for RawInput {
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

    pub struct RawInput;

    impl RawInput {
        pub fn new() -> Self {
            RawInput
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
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Stop {
    /// A key press. The animation skips to the final frame.
    Skip,
    /// Ctrl-C. The animation skips to the final frame, then `fda` exits.
    Interrupt,
}

impl Stop {
    /// Returns how the key bytes `bytes` stop the animation. In raw mode,
    /// Ctrl-C arrives as the byte 0x03.
    fn from_keys(bytes: &[u8]) -> Stop {
        if bytes.contains(&0x03) {
            Stop::Interrupt
        } else {
            Stop::Skip
        }
    }
}

const ESC: u8 = 0x1b;
const BEL: u8 = 0x07;
/// The byte that Ctrl-C sends in raw mode.
const CTRL_C: u8 = 0x03;

/// The bytes of one read from stdin, as `ReplyFilter` splits them.
#[derive(Debug, Default, PartialEq)]
struct Input {
    /// The bytes of the keys that the viewer pressed.
    keys: Vec<u8>,
    /// The reply to the background color query, if this read completed it.
    reply: Option<Vec<u8>>,
}

/// Separates the reply to the background color query from the keys that the
/// viewer presses. Both arrive on stdin. The reply can arrive after
/// `Keys::background` stops its wait, and in more than one read.
#[derive(Default)]
struct ReplyFilter {
    /// True from the query until the end of its reply.
    awaiting: bool,
    /// The bytes of the reply so far, from its `ESC ]`. Empty outside a reply.
    reply: Vec<u8>,
    /// True if the last byte was an ESC, which can be the start of the reply.
    held_esc: bool,
    /// The number of reads in a row without a byte, while the filter holds an
    /// ESC or is inside a reply.
    idle_reads: usize,
}

impl ReplyFilter {
    /// The reply to the query has about 25 bytes. A longer sequence is not the reply.
    const MAX_REPLY_LEN: usize = 64;
    /// The filter abandons a reply that stops for more than this number of
    /// reads, so that it cannot hold the keys of the viewer for long.
    /// `Keys::stop` reads one time per frame, so 15 reads are half a second.
    const MAX_IDLE_READS: usize = 15;
    /// The filter holds an ESC for this number of reads without a byte. Then
    /// the ESC is the Escape key. If the reply is cut after its ESC, the next
    /// byte of the reply can arrive one or two reads later.
    const MAX_ESC_IDLE_READS: usize = 3;

    /// Splits the bytes of one read into the keys and the reply. Ctrl-C is
    /// always a key.
    fn split(&mut self, bytes: &[u8]) -> Input {
        let mut input = Input::default();
        self.idle_reads = 0;
        for &b in bytes {
            if !self.awaiting || b == CTRL_C {
                input.keys.push(b);
            } else if !self.reply.is_empty() {
                self.reply.push(b);
                if b == BEL || self.reply.ends_with(&[ESC, b'\\']) {
                    input.reply = Some(std::mem::take(&mut self.reply));
                    self.awaiting = false;
                } else if self.reply.len() > Self::MAX_REPLY_LEN {
                    self.abandon();
                }
            } else if self.held_esc {
                self.held_esc = false;
                if b == b']' {
                    self.reply.extend([ESC, b']']);
                } else {
                    // The ESC was a key, or the start of the sequence of a key.
                    input.keys.push(ESC);
                    if b == ESC {
                        self.held_esc = true;
                    } else {
                        input.keys.push(b);
                    }
                }
            } else if b == ESC {
                self.held_esc = true;
            } else {
                input.keys.push(b);
            }
        }
        input
    }

    /// Handles a read without a byte. Returns an ESC as a key if `split` held
    /// it for too long. Abandons a reply that stops for too long.
    fn idle(&mut self) -> Input {
        let mut input = Input::default();
        if self.held_esc {
            self.idle_reads += 1;
            if self.idle_reads > Self::MAX_ESC_IDLE_READS {
                self.held_esc = false;
                input.keys.push(ESC);
            }
        } else if !self.reply.is_empty() {
            self.idle_reads += 1;
            if self.idle_reads > Self::MAX_IDLE_READS {
                self.abandon();
            }
        }
        input
    }

    /// Stops the wait for the reply and drops the bytes of the reply so far.
    fn abandon(&mut self) {
        self.awaiting = false;
        self.reply.clear();
        self.idle_reads = 0;
    }
}

/// The keys that the viewer presses while the animation plays. While a `Keys`
/// exists, stdin is in raw mode.
pub(super) struct Keys {
    raw: RawInput,
    filter: ReplyFilter,
}

impl Keys {
    pub(super) fn new() -> Self {
        Keys {
            raw: RawInput::new(),
            filter: ReplyFilter::default(),
        }
    }

    /// Returns how the viewer stopped the animation since the last call, or
    /// `None` if the viewer pressed no key.
    pub(super) fn stop(&mut self) -> Option<Stop> {
        let input = match self.raw.pressed() {
            Some(bytes) => self.filter.split(&bytes),
            None => self.filter.idle(),
        };
        (!input.keys.is_empty()).then(|| Stop::from_keys(&input.keys))
    }

    /// Asks the terminal for its background color with an OSC 11 query. Waits up to 150 ms,
    /// because a terminal that does not support OSC 11 does not answer. A reply that arrives
    /// after the wait is not a key, and `stop` drops it.
    pub(super) fn background(&mut self, out: &mut impl Write) -> Option<Theme> {
        if !self.raw.is_raw() {
            // Without raw mode, the terminal echoes the reply on the screen.
            return None;
        }
        let _ = write!(out, "\x1b]11;?\x1b\\");
        let _ = out.flush();
        self.filter.awaiting = true;
        let deadline = Instant::now() + Duration::from_millis(150);
        while Instant::now() < deadline {
            match self.raw.pressed() {
                // This wait drops the keys that the viewer presses in it.
                Some(bytes) => {
                    if let Some(reply) = self.filter.split(&bytes).reply {
                        return Theme::from_osc11(&reply);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ctrl_c_interrupts_and_other_keys_skip() {
        assert_eq!(Stop::from_keys(b" "), Stop::Skip);
        assert_eq!(Stop::from_keys(b"\x1b[A"), Stop::Skip, "arrow key");
        assert_eq!(Stop::from_keys(b"\x03"), Stop::Interrupt);
        assert_eq!(
            Stop::from_keys(b"q\x03"),
            Stop::Interrupt,
            "Ctrl-C after a key"
        );
    }

    /// Returns a filter that waits for the reply to a query.
    fn waiting_filter() -> ReplyFilter {
        ReplyFilter {
            awaiting: true,
            ..ReplyFilter::default()
        }
    }

    const REPLY: &[u8] = b"\x1b]11;rgb:0000/0000/0000\x1b\\";

    #[test]
    fn reply_is_not_a_key() {
        let mut filter = waiting_filter();
        let input = filter.split(REPLY);
        assert_eq!(input.keys, b"");
        assert_eq!(input.reply.as_deref(), Some(REPLY));
    }

    #[test]
    fn reply_in_more_than_one_read_is_not_a_key() {
        let mut filter = waiting_filter();
        // The reply is cut after its ESC, in its text, and before its BEL.
        for part in [&b"\x1b"[..], b"]11;rgb:00", b"00/0000/0000"] {
            assert_eq!(filter.split(part), Input::default(), "{part:?}");
            assert_eq!(filter.idle(), Input::default(), "idle after {part:?}");
        }
        let end = filter.split(b"\x07");
        assert_eq!(end.keys, b"");
        assert_eq!(
            end.reply.as_deref(),
            Some(&b"\x1b]11;rgb:0000/0000/0000\x07"[..])
        );
    }

    #[test]
    fn keys_before_and_after_the_reply_are_keys() {
        let mut filter = waiting_filter();
        let input = filter.split(b"q\x1b]11;rgb:f/f/f\x07w");
        assert_eq!(input.keys, b"qw");
        assert!(input.reply.is_some());
        // After the reply, the filter passes each byte.
        assert_eq!(filter.split(REPLY).keys, REPLY);
    }

    #[test]
    fn escape_keys_are_keys() {
        let mut filter = waiting_filter();
        assert_eq!(filter.split(b"\x1b[A").keys, b"\x1b[A", "arrow key");
        // The filter holds an ESC at the end of a read. After some reads
        // without a byte, the ESC is the Escape key.
        assert_eq!(filter.split(b"\x1b").keys, b"");
        for _ in 0..ReplyFilter::MAX_ESC_IDLE_READS {
            assert_eq!(filter.idle(), Input::default());
        }
        assert_eq!(filter.idle().keys, b"\x1b", "the Escape key");
        assert_eq!(filter.split(b"\x1b\x1b").keys, b"\x1b");
        assert_eq!(filter.split(b"x").keys, b"\x1bx");
    }

    #[test]
    fn each_byte_is_a_key_without_a_query() {
        let mut filter = ReplyFilter::default();
        assert_eq!(filter.split(REPLY).keys, REPLY);
        assert_eq!(filter.idle(), Input::default());
    }

    #[test]
    fn filter_does_not_hold_the_keys_of_the_viewer() {
        // Ctrl-C is a key inside a reply.
        let mut filter = waiting_filter();
        assert_eq!(filter.split(b"\x1b]11;rgb\x03").keys, [CTRL_C]);

        // A sequence that is longer than `MAX_REPLY_LEN` ends the wait.
        let mut filter = waiting_filter();
        let long = [&b"\x1b]"[..], &[b'x'; ReplyFilter::MAX_REPLY_LEN - 1]].concat();
        assert_eq!(filter.split(&long), Input::default());
        assert_eq!(filter.split(b"q").keys, b"q");

        // A reply that stops ends the wait after `MAX_IDLE_READS` reads.
        let mut filter = waiting_filter();
        filter.split(b"\x1b]11;rgb:");
        for _ in 0..ReplyFilter::MAX_IDLE_READS {
            filter.idle();
        }
        assert_eq!(filter.split(b"q").keys, b"", "the wait ended too soon");
        // The reply takes the `q`. The reads without a byte start again.
        for _ in 0..=ReplyFilter::MAX_IDLE_READS {
            filter.idle();
        }
        assert_eq!(filter.split(b"q").keys, b"q");
    }
}
