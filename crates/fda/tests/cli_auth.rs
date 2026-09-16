//! Credential selection at the command line: one credential at a time, a
//! header the user writes out in full, and a note for shells that still export
//! the variable read before 0.339.0.

use std::process::{Command, Output};

/// `fda pipelines` against a port nothing listens on, with the credential
/// variables cleared so a developer shell cannot leak into a case.
fn fda() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_fda"));
    for var in [
        "FELDERA_HOST",
        "FELDERA_API_KEY",
        "FELDERA_OIDC_TOKEN_FILE",
        "FELDERA_AUTH_TOKEN_COMMAND",
    ] {
        command.env_remove(var);
    }
    command.args([
        "--host",
        "https://127.0.0.1:1",
        "--retries",
        "0",
        "--timeout",
        "5",
        "pipelines",
    ]);
    command
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn assert_conflict(output: Output) {
    assert_eq!(output.status.code(), Some(2), "{}", stderr(&output));
    assert!(
        stderr(&output).contains("cannot be used with"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn auth_and_token_file_conflict_as_flags() {
    let output = fda()
        .args(["--auth", "apikey:x", "--oidc-token-file", "/t"])
        .output()
        .expect("run fda");
    assert_conflict(output);
}

#[test]
fn auth_and_token_file_conflict_from_env() {
    let output = fda()
        .env("FELDERA_API_KEY", "apikey:x")
        .env("FELDERA_OIDC_TOKEN_FILE", "/t")
        .output()
        .expect("run fda");
    assert_conflict(output);
}

#[test]
fn removed_token_command_variable_is_called_out() {
    let output = fda()
        .env("FELDERA_AUTH_TOKEN_COMMAND", "true")
        .output()
        .expect("run fda");
    assert!(
        stderr(&output).contains("FELDERA_AUTH_TOKEN_COMMAND is no longer read"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn removed_token_command_variable_is_silent_next_to_a_credential() {
    let output = fda()
        .env("FELDERA_AUTH_TOKEN_COMMAND", "true")
        .env("FELDERA_API_KEY", "apikey:x")
        .output()
        .expect("run fda");
    assert!(
        !stderr(&output).contains("FELDERA_AUTH_TOKEN_COMMAND"),
        "{}",
        stderr(&output)
    );
}

/// `--header` is global, so it parses after the subcommand as well, which is
/// where a long cookie tends to land on the command line.
#[test]
fn header_parses_after_the_subcommand() {
    let output = fda()
        .args(["-H", "Cookie: session=abc"])
        .output()
        .expect("run fda");
    // The request then fails to connect, which is exit 1; a parse error is 2.
    assert_eq!(output.status.code(), Some(1), "{}", stderr(&output));
    assert!(
        !stderr(&output).contains("unexpected argument"),
        "{}",
        stderr(&output)
    );
}

/// An argument that is not `Name: Value` is reported with the argument quoted,
/// rather than sent as a malformed header.
#[test]
fn unparsable_header_names_the_argument() {
    let output = fda().args(["-H", "bogus"]).output().expect("run fda");
    let stderr = stderr(&output);
    assert_eq!(output.status.code(), Some(1), "{stderr}");
    assert!(stderr.contains("bogus"), "{stderr}");
    assert!(stderr.contains("Name: Value"), "{stderr}");
}
