//! `fda logs` without `--watch` prints the retained logs and exits, even for a
//! pipeline that never stops logging.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// Answers `/logs?follow=false` with two lines and ends the response, and any
/// other `/logs` request with a line every 100 ms until the client leaves.
fn serve_logs(stream: TcpStream) {
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
    let mut request_line = String::new();
    reader.read_line(&mut request_line).expect("read request");
    // Skip the headers; a GET has no body.
    let mut header = String::new();
    while reader.read_line(&mut header).expect("read header") > 2 {
        header.clear();
    }

    let mut stream = stream;
    let target = request_line.split(' ').nth(1).unwrap_or_default();
    if !target.starts_with("/v0/pipelines/p/logs") {
        let _ = stream.write_all(b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\n\r\n");
        return;
    }
    let head = "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ntransfer-encoding: chunked\r\n\r\n";
    let _ = stream.write_all(head.as_bytes());
    let chunk = |line: &str| format!("{:x}\r\n{line}\r\n", line.len());
    let _ = stream.write_all(format!("{}{}", chunk("one\n"), chunk("two\n")).as_bytes());
    if target.contains("follow=false") {
        let _ = stream.write_all(b"0\r\n\r\n");
        return;
    }
    while stream.write_all(chunk("more\n").as_bytes()).is_ok() {
        thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn logs_without_watch_exits_on_a_busy_pipeline() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let host = format!("http://{}", listener.local_addr().expect("local address"));
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            thread::spawn(move || serve_logs(stream));
        }
    });

    let mut child = Command::new(env!("CARGO_BIN_EXE_fda"))
        .env_remove("FELDERA_API_KEY")
        .env_remove("FELDERA_OIDC_TOKEN_FILE")
        .env_remove("FELDERA_AUTH_TOKEN_COMMAND")
        .args(["--host", &host, "--retries", "0", "logs", "p"])
        .stdout(Stdio::piped())
        .spawn()
        .expect("run fda");

    // The server never pauses, so only the end of the response can stop fda.
    let deadline = Instant::now() + Duration::from_secs(30);
    while child.try_wait().expect("wait for fda").is_none() {
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("fda logs did not exit on a pipeline that logs all the time");
        }
        thread::sleep(Duration::from_millis(100));
    }
    let mut stdout = String::new();
    child
        .stdout
        .take()
        .expect("stdout")
        .read_to_string(&mut stdout)
        .expect("read stdout");
    assert_eq!(stdout, "one\ntwo\n");
}
