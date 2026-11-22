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
