//! `fda welcome` and the first-run check when stdout is a pipe. The animation
//! itself needs a terminal, so these tests cover only the output without one.

use std::net::TcpListener;
use std::path::Path;
use std::process::Command;

fn fda() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fda"));
    command
        .env_remove("FELDERA_API_KEY")
        .env_remove("FELDERA_OIDC_TOKEN_FILE")
        .env_remove("FELDERA_AUTH_TOKEN_COMMAND");
    command
}

/// Returns true if a file named `welcome-shown` is in `dir` or below it.
fn has_marker(dir: &Path) -> bool {
    std::fs::read_dir(dir)
        .expect("read dir")
        .flatten()
        .any(|entry| {
            let path = entry.path();
            path.file_name().is_some_and(|name| name == "welcome-shown")
                || (path.is_dir() && has_marker(&path))
        })
}

#[test]
fn welcome_in_a_pipe_prints_the_window_without_escape_sequences() {
    let output = fda()
        .args(["--host", "https://try.feldera.com/", "welcome"])
        .output()
        .expect("run fda");
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).expect("UTF-8 output");
    assert!(!text.contains('\x1b'), "escape sequences in a pipe");
    // Without a terminal, fda assumes 80 columns. The window is then 78
    // columns wide and 30 rows high.
    let rows: Vec<&str> = text.lines().collect();
    assert_eq!(rows.len(), 30);
    assert!(
        rows.iter().all(|row| row.chars().count() == 78),
        "ragged rows"
    );
    for part in [
        "Precisely compute only what changed.",
        "GET STARTED",
        "try.feldera.com",
    ] {
        assert!(text.contains(part), "{part:?} is missing");
    }
}

#[test]
fn first_run_in_a_pipe_plays_nothing_and_writes_no_marker() {
    let home = tempfile::tempdir().expect("temp dir");
    // No server listens on this port, so the command fails right after the
    // first-run check.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("local address").port();
    drop(listener);

    let output = fda()
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", home.path().join("config"))
        .args([
            "--host",
            &format!("http://127.0.0.1:{port}"),
            "--retries",
            "0",
            "pipelines",
        ])
        .output()
        .expect("run fda");
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        !text.contains('╭') && !text.contains("GET STARTED"),
        "the window is in the pipe"
    );
    assert!(!has_marker(home.path()), "the first run is claimed");
}

/// Tests that run fda in a pseudo-terminal.
#[cfg(unix)]
mod tty {
    use super::fda;
    use std::fs::File;
    use std::io::{Read, Write};
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::path::Path;
    use std::process::{Child, ExitStatus, Stdio};
    use std::sync::{Arc, Mutex};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    /// The escape sequence that hides the cursor. fda sends it when the
    /// animation starts.
    const ANIMATION_STARTED: &str = "\x1b[?25l";
    /// The start of the query for the background color of the terminal.
    const BACKGROUND_QUERY: &str = "\x1b]11;?";
    const WELCOME: [&str; 3] = ["--host", "https://try.feldera.com/", "welcome"];

    /// An fda process in a pseudo-terminal.
    struct Session {
        child: Child,
        /// The side of the pseudo-terminal that the test reads and writes.
        terminal: File,
        /// The bytes that fda wrote so far.
        output: Arc<Mutex<Vec<u8>>>,
        reader: Option<JoinHandle<()>>,
    }

    impl Session {
        /// Starts fda with `args` in a pseudo-terminal of `cols` by `rows`.
        /// `home` is the home directory of fda.
        fn start(cols: u16, rows: u16, home: &Path, args: &[&str]) -> Session {
            let (mut master, mut slave) = (0, 0);
            let mut size = libc::winsize {
                ws_row: rows,
                ws_col: cols,
                ws_xpixel: 0,
                ws_ypixel: 0,
            };
            // SAFETY: openpty writes one file descriptor through each of the
            // first two pointers, which point to live integers. `size` is a
            // live `winsize`. Null is valid for the name and for the settings.
            let status = unsafe {
                libc::openpty(
                    &mut master,
                    &mut slave,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    &mut size,
                )
            };
            assert_eq!(status, 0, "openpty failed");
            // SAFETY: openpty opened both file descriptors, and no other code owns them.
            let (terminal, slave) =
                unsafe { (File::from_raw_fd(master), OwnedFd::from_raw_fd(slave)) };
            let stdio = || Stdio::from(slave.try_clone().expect("clone the terminal"));
            let child = fda()
                .args(args)
                .env("HOME", home)
                .env("XDG_CONFIG_HOME", home.join("config"))
                .env("TERM", "xterm-256color")
                .env("COLORTERM", "truecolor")
                .env_remove("CI")
                .env_remove("FDA_NO_ANIMATION")
                .env_remove("NO_COLOR")
                .env_remove("COLORFGBG")
                .stdin(stdio())
                .stdout(stdio())
                .stderr(stdio())
                .spawn()
                .expect("run fda");
            // Only fda has the other side open now, so the reader ends when fda exits.
            drop(slave);
            let output = Arc::new(Mutex::new(Vec::new()));
            let mut source = terminal.try_clone().expect("clone the terminal");
            let sink = Arc::clone(&output);
            let reader = thread::spawn(move || {
                let mut buf = [0u8; 4096];
                while let Ok(n @ 1..) = source.read(&mut buf) {
                    sink.lock().unwrap().extend_from_slice(&buf[..n]);
                }
            });
            Session {
                child,
                terminal,
                output,
                reader: Some(reader),
            }
        }

        /// Types `bytes` into the terminal.
        fn send(&mut self, bytes: &[u8]) {
            self.terminal
                .write_all(bytes)
                .expect("write to the terminal");
        }

        /// Returns the output so far, with its escape sequences.
        fn raw(&self) -> String {
            String::from_utf8_lossy(&self.output.lock().unwrap()).into_owned()
        }

        /// Returns the output so far, without its escape sequences.
        fn text(&self) -> String {
            plain(&self.raw())
        }

        /// Waits until the output has the escape sequence or text `part`.
        /// Panics after 10 seconds.
        fn wait_for(&self, part: &str) {
            let limit = Instant::now() + Duration::from_secs(10);
            while !self.raw().contains(part) {
                assert!(Instant::now() < limit, "no {part:?} in the output");
                thread::sleep(Duration::from_millis(5));
            }
        }

        fn is_running(&mut self) -> bool {
            self.child.try_wait().expect("poll fda").is_none()
        }

        /// Waits for fda to exit and for the rest of its output. Panics if fda
        /// still runs after 20 seconds.
        fn finish(&mut self) -> ExitStatus {
            let limit = Instant::now() + Duration::from_secs(20);
            loop {
                if let Some(status) = self.child.try_wait().expect("poll fda") {
                    if let Some(reader) = self.reader.take() {
                        reader.join().expect("join the reader");
                    }
                    return status;
                }
                assert!(Instant::now() < limit, "fda did not exit");
                thread::sleep(Duration::from_millis(5));
            }
        }

        /// Returns true if the terminal echoes keys, buffers lines and changes
        /// Ctrl-C into a signal, as it did before fda started.
        fn is_restored(&self) -> bool {
            // SAFETY: `termios` is a plain C struct of integers, so all zeroes is a valid value.
            let mut t: libc::termios = unsafe { std::mem::zeroed() };
            // SAFETY: tcgetattr writes one `termios` through the pointer, which
            // points to `t`. The file descriptor is open.
            let ok = unsafe { libc::tcgetattr(self.terminal.as_raw_fd(), &mut t) } == 0;
            let normal = libc::ECHO | libc::ICANON | libc::ISIG;
            ok && t.c_lflag & normal == normal
        }
    }

    impl Drop for Session {
        /// Stops fda if a failed assertion ends the test while fda still runs.
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    /// Returns `raw` without its escape sequences.
    fn plain(raw: &str) -> String {
        let mut out = String::new();
        let mut chars = raw.chars();
        while let Some(c) = chars.next() {
            if c != '\x1b' {
                out.push(c);
                continue;
            }
            match chars.next() {
                // A CSI sequence ends with a character from `@` to `~`.
                Some('[') => {
                    chars.by_ref().find(|c| ('@'..='~').contains(c));
                }
                // An OSC sequence ends with BEL, or with ESC and a backslash.
                Some(']') => {
                    let end = chars.by_ref().find(|c| matches!(c, '\x07' | '\x1b'));
                    if end == Some('\x1b') {
                        chars.next();
                    }
                }
                _ => {}
            }
        }
        out
    }

    #[test]
    fn a_key_skips_the_animation_and_the_terminal_is_restored() {
        let home = tempfile::tempdir().expect("temp dir");
        let mut session = Session::start(100, 40, home.path(), &WELCOME);
        session.wait_for(ANIMATION_STARTED);
        session.send(b" ");
        assert!(session.finish().success());
        let text = session.text();
        // The key arrives in the intro, so the film and its skip hint do not play.
        assert!(!text.contains("press any key to skip"), "the film played");
        for part in [
            "Precisely compute only what changed.",
            "GET STARTED",
            "try.feldera.com",
        ] {
            assert!(text.contains(part), "{part:?} is missing");
        }
        assert!(session.raw().ends_with("\x1b[?25h"), "the cursor is hidden");
        assert!(session.is_restored(), "the terminal is in raw mode");
    }

    #[test]
    fn late_background_reply_is_not_a_key() {
        let home = tempfile::tempdir().expect("temp dir");
        let mut session = Session::start(100, 40, home.path(), &WELCOME);
        session.wait_for(BACKGROUND_QUERY);
        // fda waits 150 ms for the reply. This reply arrives after that time.
        thread::sleep(Duration::from_millis(400));
        session.send(b"\x1b]11;rgb:0000/0000/0000\x1b\\");
        thread::sleep(Duration::from_millis(600));
        assert!(session.is_running(), "the late reply ended the animation");
        assert!(
            !session.text().contains("Precisely"),
            "the late reply skipped the film"
        );
        session.send(b" ");
        assert!(session.finish().success());
    }

    #[test]
    fn background_reply_on_time_selects_the_theme() {
        // A slow computer can miss the 150 ms that fda waits for the reply,
        // so the test makes three attempts.
        let is_selected = (0..3).any(|_| {
            let home = tempfile::tempdir().expect("temp dir");
            let mut session = Session::start(100, 40, home.path(), &WELCOME);
            session.wait_for(BACKGROUND_QUERY);
            // The reply for a white background. fda then draws secondary text
            // in Lichen shade.
            session.send(b"\x1b]11;rgb:ffff/ffff/ffff\x1b\\");
            session.wait_for(ANIMATION_STARTED);
            session.send(b" ");
            assert!(session.finish().success());
            session.raw().contains("\x1b[38;2;99;99;61m")
        });
        assert!(is_selected, "no Lichen shade in the output");
    }
}
