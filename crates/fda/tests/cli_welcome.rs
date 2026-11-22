//! `fda welcome` when stdout is a pipe. The animation itself needs a terminal.

use std::process::Command;

fn fda() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fda"));
    command
        .env_remove("FELDERA_API_KEY")
        .env_remove("FELDERA_OIDC_TOKEN_FILE")
        .env_remove("FELDERA_AUTH_TOKEN_COMMAND");
    command
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
