//! The terminal event loop: owns the terminal, the poller, and the reducer.

use anyhow::{Context, Result};
use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
    MouseEvent, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::mpsc;

use crate::app::{App, Cmd, KeyPress, Msg, update};
use crate::browser::{Browser, Revealer};
use crate::gateway::{DownloadKind, Gateway};
use crate::options::UiSettings;
use crate::poll::{self, PollControl};

/// Animation and toast-expiry cadence.
const TICK_MILLIS: u64 = 150;

/// Map a crossterm key event to the reducer's key alphabet.
pub fn key_press(key: KeyEvent) -> Option<KeyPress> {
    if !matches!(key.kind, KeyEventKind::Press | KeyEventKind::Repeat) {
        return None;
    }
    if key.modifiers.contains(KeyModifiers::SHIFT) {
        match key.code {
            KeyCode::Up => return Some(KeyPress::ShiftUp),
            KeyCode::Down => return Some(KeyPress::ShiftDown),
            _ => {}
        }
    }
    match key.code {
        KeyCode::Up => Some(KeyPress::Up),
        KeyCode::Down => Some(KeyPress::Down),
        KeyCode::PageUp => Some(KeyPress::PageUp),
        KeyCode::PageDown => Some(KeyPress::PageDown),
        KeyCode::Home => Some(KeyPress::Home),
        KeyCode::End => Some(KeyPress::End),
        KeyCode::Left => Some(KeyPress::Left),
        KeyCode::Right => Some(KeyPress::Right),
        KeyCode::Enter => Some(KeyPress::Enter),
        KeyCode::Esc => Some(KeyPress::Escape),
        KeyCode::Backspace => Some(KeyPress::Backspace),
        KeyCode::Tab => Some(KeyPress::Tab),
        KeyCode::BackTab => Some(KeyPress::BackTab),
        KeyCode::Char(character) => Some(KeyPress::Char(character)),
        _ => None,
    }
}

/// Map a mouse event to the reducer's key alphabet; only the wheel and the
/// left button matter.
pub fn mouse_press(mouse: MouseEvent) -> Option<KeyPress> {
    match mouse.kind {
        MouseEventKind::ScrollUp => Some(KeyPress::ScrollUp),
        MouseEventKind::ScrollDown => Some(KeyPress::ScrollDown),
        MouseEventKind::Down(crossterm::event::MouseButton::Left) => Some(KeyPress::Click {
            row: mouse.row,
            column: mouse.column,
        }),
        _ => None,
    }
}

/// Whether the event ends the program unconditionally.
pub fn is_quit_event(key: &KeyEvent) -> bool {
    key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL)
}

/// File name for a saved download: the pipeline, the kind, and the instant,
/// with a shell-safe pipeline name.
///
/// ```
/// use f5a::gateway::DownloadKind;
/// use f5a::runner::download_file_name;
///
/// assert_eq!(
///     download_file_name(DownloadKind::SamplyProfile, "orders", 1_700),
///     "orders-samply-1700.json.gz"
/// );
/// assert_eq!(
///     download_file_name(DownloadKind::SupportBundle, "a b", 1_700),
///     "a_b-support-bundle-1700.zip"
/// );
/// ```
pub fn download_file_name(kind: DownloadKind, pipeline: &str, now_millis: i64) -> String {
    let safe: String = pipeline
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect();
    let (infix, extension) = kind.file_parts();
    format!("{safe}-{infix}-{now_millis}.{extension}")
}

/// Executes reducer commands: spawns API calls, steers the poller.
pub struct Dispatcher<G: Gateway> {
    gateway: G,
    messages: mpsc::UnboundedSender<Msg>,
    poller: mpsc::UnboundedSender<PollControl>,
    /// Directory receiving downloads such as Samply profiles (`--download-dir`);
    /// created on first use.
    output_dir: std::path::PathBuf,
    /// The `samply` executable that serves saved profiles; a bare name
    /// resolves through PATH.
    samply_binary: std::path::PathBuf,
    /// The browser that shows served profiles.
    browser: Browser,
    /// The file manager command that shows saved support bundles.
    revealer: Revealer,
    /// The `samply load` servers f5a started, by profile path. One server per
    /// profile; all of them die with f5a.
    viewers: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, Viewer>>>,
    /// The active log-follow task; replaced or aborted through StartLogs and
    /// StopLogs.
    log_stream: std::sync::Mutex<Option<tokio::task::AbortHandle>>,
}

/// A `samply load` server f5a started for one profile.
enum Viewer {
    /// The launch is in flight; a repeat request meanwhile is ignored.
    Starting,
    Serving {
        server: tokio::process::Child,
        url: String,
    },
}

/// How long `samply load` may take to print the profile URL.
const VIEWER_START_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Start `samply load --no-open <profile>` and wait for the profiler URL it
/// prints on stdout. samply's own opener would use the system default, which
/// on a Mac is often Safari, where the Firefox Profiler does not run, so the
/// caller opens the URL itself. The server dies with its handle (and so with
/// f5a); the profile stays on disk for `samply load` by hand.
async fn launch_samply_viewer(
    binary: &std::path::Path,
    profile: &str,
) -> Result<(tokio::process::Child, String), String> {
    use tokio::io::AsyncBufReadExt;
    let mut viewer = tokio::process::Command::new(binary)
        .arg("load")
        .arg("--no-open")
        .arg(profile)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| format!("could not start `{} load`: {error}", binary.display()))?;
    let stdout = viewer
        .stdout
        .take()
        .expect("stdout was requested as a pipe");
    let mut lines = tokio::io::BufReader::new(stdout).lines();
    let url = loop {
        match tokio::time::timeout(VIEWER_START_TIMEOUT, lines.next_line()).await {
            Ok(Ok(Some(line))) if line.trim().starts_with("http") => break line.trim().to_string(),
            Ok(Ok(Some(_))) => continue,
            Ok(Ok(None)) => {
                let status = match viewer.wait().await {
                    Ok(status) => status.to_string(),
                    Err(error) => error.to_string(),
                };
                return Err(format!(
                    "`{} load` exited early with {status}; run it in a shell to see why",
                    binary.display()
                ));
            }
            Ok(Err(error)) => {
                return Err(format!(
                    "could not read `{} load` output: {error}",
                    binary.display()
                ));
            }
            Err(_elapsed) => {
                return Err(format!(
                    "`{} load` printed no profile URL within {}s",
                    binary.display(),
                    VIEWER_START_TIMEOUT.as_secs()
                ));
            }
        }
    };
    // Keep draining so the server never blocks on a full pipe.
    tokio::spawn(async move { while let Ok(Some(_)) = lines.next_line().await {} });
    Ok((viewer, url))
}

/// Show `path` in the file manager, detached.
fn reveal_file(revealer: &Revealer, path: &str) -> Result<(), String> {
    let argv = revealer.argv(path);
    tokio::process::Command::new(&argv[0])
        .args(&argv[1..])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|error| {
            format!(
                "could not start {} (`{}`): {error}",
                revealer.label, argv[0]
            )
        })?;
    Ok(())
}

/// Hand `url` to the browser, detached. Returns the browser's label.
fn open_in_browser(browser: &Browser, url: &str) -> Result<String, String> {
    let argv = browser.argv(url);
    tokio::process::Command::new(&argv[0])
        .args(&argv[1..])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|error| {
            format!(
                "could not start {} (`{}`): {error}; open {url} yourself",
                browser.label, argv[0]
            )
        })?;
    Ok(browser.label.clone())
}

impl<G: Gateway> Dispatcher<G> {
    pub fn new(
        gateway: G,
        messages: mpsc::UnboundedSender<Msg>,
        poller: mpsc::UnboundedSender<PollControl>,
        output_dir: std::path::PathBuf,
        samply_binary: std::path::PathBuf,
        browser: Browser,
        revealer: Revealer,
    ) -> Self {
        Self {
            gateway,
            messages,
            poller,
            output_dir,
            samply_binary,
            browser,
            revealer,
            viewers: std::sync::Arc::default(),
            log_stream: std::sync::Mutex::new(None),
        }
    }

    fn stop_log_stream(&self) {
        if let Some(handle) = self.log_stream.lock().expect("log stream lock").take() {
            handle.abort();
        }
    }

    /// Stop everything f5a started in the background: the log stream and the
    /// samply servers. Open profiler tabs keep what they loaded but cannot
    /// reload; `samply load <file>` brings a profile back.
    pub fn shutdown(&self) {
        self.stop_log_stream();
        for (_, viewer) in self.viewers.lock().expect("viewer lock").drain() {
            if let Viewer::Serving { mut server, .. } = viewer {
                // A server that already exited needs no signal.
                let _ = server.start_kill();
            }
        }
    }

    /// Show `path` in the browser: through the server already serving it, or
    /// through a new one when none is alive.
    fn open_viewer(&self, path: String) {
        let mut viewers = self.viewers.lock().expect("viewer lock");
        let serving_url = match viewers.get_mut(&path) {
            Some(Viewer::Starting) => return,
            Some(Viewer::Serving { server, url }) => match server.try_wait() {
                Ok(None) => Some(url.clone()),
                _ => None,
            },
            None => None,
        };
        if let Some(url) = serving_url {
            let result = open_in_browser(&self.browser, &url);
            let _ = self.messages.send(Msg::SamplyOpened { path, result });
            return;
        }
        viewers.insert(path.clone(), Viewer::Starting);
        drop(viewers);
        let messages = self.messages.clone();
        let binary = self.samply_binary.clone();
        let browser = self.browser.clone();
        let registry = std::sync::Arc::clone(&self.viewers);
        tokio::spawn(async move {
            let result = match launch_samply_viewer(&binary, &path).await {
                Ok((server, url)) => {
                    let opened = open_in_browser(&browser, &url);
                    registry
                        .lock()
                        .expect("viewer lock")
                        .insert(path.clone(), Viewer::Serving { server, url });
                    opened
                }
                Err(error) => {
                    registry.lock().expect("viewer lock").remove(&path);
                    Err(error)
                }
            };
            let _ = messages.send(Msg::SamplyOpened { path, result });
        });
    }

    /// Execute one command; returns `true` when the program should exit.
    pub fn dispatch(&self, command: Cmd) -> bool {
        match command {
            Cmd::Refresh => {
                let _ = self.poller.send(PollControl::RefreshNow);
            }
            Cmd::Watch(pipeline) => {
                let _ = self.poller.send(PollControl::Watch(pipeline));
            }
            Cmd::SetRefreshSecs(seconds) => {
                let _ = self.poller.send(PollControl::SetRefreshSecs(seconds));
            }
            Cmd::Run(action) => {
                let gateway = self.gateway.clone();
                let messages = self.messages.clone();
                tokio::spawn(async move {
                    let result = gateway.execute(action.clone()).await;
                    let _ = messages.send(Msg::ActionFinished { action, result });
                });
            }
            Cmd::FetchSql(pipeline) => {
                let gateway = self.gateway.clone();
                let messages = self.messages.clone();
                tokio::spawn(async move {
                    let result = gateway.program_sql(&pipeline).await;
                    let _ = messages.send(Msg::SqlLoaded { pipeline, result });
                });
            }
            Cmd::FetchHotspots(pipeline) => {
                let gateway = self.gateway.clone();
                let messages = self.messages.clone();
                tokio::spawn(async move {
                    let result = gateway.hotspots(&pipeline).await;
                    let _ = messages.send(Msg::HotspotsLoaded { pipeline, result });
                });
            }
            Cmd::FetchDownload {
                kind,
                pipeline,
                bundle,
            } => {
                let gateway = self.gateway.clone();
                let messages = self.messages.clone();
                tokio::spawn(async move {
                    let result = gateway.download(kind, &pipeline, bundle).await;
                    let _ = messages.send(Msg::DownloadFetched {
                        kind,
                        pipeline,
                        result,
                    });
                });
            }
            Cmd::SaveDownload {
                kind,
                pipeline,
                bytes,
            } => {
                let messages = self.messages.clone();
                let file_path =
                    self.output_dir
                        .join(download_file_name(kind, &pipeline, poll::now_millis()));
                let path = file_path.display().to_string();
                tokio::spawn(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        let directory = file_path
                            .parent()
                            .expect("a download path always names a directory");
                        std::fs::create_dir_all(directory)
                            .and_then(|()| std::fs::write(&file_path, bytes))
                            .map_err(|error| error.to_string())
                    })
                    .await
                    .unwrap_or_else(|join_error| Err(join_error.to_string()));
                    let _ = messages.send(Msg::FileSaved {
                        kind,
                        pipeline,
                        path,
                        result,
                    });
                });
            }
            Cmd::FetchDiagnostics(pipeline) => {
                let gateway = self.gateway.clone();
                let messages = self.messages.clone();
                tokio::spawn(async move {
                    let result = gateway.diagnostics(&pipeline).await;
                    let _ = messages.send(Msg::DiagnosticsLoaded { pipeline, result });
                });
            }
            Cmd::StartLogs(pipeline) => {
                self.stop_log_stream();
                let gateway = self.gateway.clone();
                let messages = self.messages.clone();
                let task = tokio::spawn(async move {
                    let (line_tx, mut line_rx) = mpsc::unbounded_channel::<String>();
                    let stream_pipeline = pipeline.clone();
                    let stream_gateway = gateway.clone();
                    let stream = tokio::spawn(async move {
                        stream_gateway.stream_logs(&stream_pipeline, line_tx).await
                    });
                    while let Some(line) = line_rx.recv().await {
                        let _ = messages.send(Msg::LogLine {
                            pipeline: pipeline.clone(),
                            line,
                        });
                    }
                    let result = stream.await.unwrap_or_else(|join_error| {
                        Err(crate::http::ApiError::Transport(join_error.to_string()))
                    });
                    let _ = messages.send(Msg::LogsClosed { pipeline, result });
                });
                *self.log_stream.lock().expect("log stream lock") = Some(task.abort_handle());
            }
            Cmd::StopLogs => self.stop_log_stream(),
            Cmd::OpenSamply { path } => self.open_viewer(path),
            Cmd::RevealFile { path } => {
                let result = reveal_file(&self.revealer, &path);
                let _ = self.messages.send(Msg::FileRevealed { path, result });
            }
            Cmd::SwitchTenant(tenant) => {
                self.gateway.set_tenant(tenant);
            }
            Cmd::Quit => return true,
        }
        false
    }

    /// Run all commands; returns `true` when any of them quits.
    pub fn dispatch_all(&self, commands: Vec<Cmd>) -> bool {
        let mut quit = false;
        for command in commands {
            quit |= self.dispatch(command);
        }
        quit
    }
}

/// Restores the terminal even when the draw loop errors or panics.
struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode().context("could not enable raw terminal mode")?;
        // Mouse capture makes the wheel scroll our lists instead of letting
        // the terminal fake arrow keys (three per notch, which used to make
        // the selection jump).
        if let Err(error) = execute!(std::io::stdout(), EnterAlternateScreen, EnableMouseCapture) {
            let _ = disable_raw_mode();
            return Err(error).context("could not enter the alternate screen");
        }
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(std::io::stdout(), DisableMouseCapture, LeaveAlternateScreen);
    }
}

/// Run the console until the user quits.
pub async fn run<G: Gateway>(gateway: G, settings: UiSettings) -> Result<()> {
    let guard = TerminalGuard::enter()?;
    let mut terminal =
        Terminal::new(CrosstermBackend::new(std::io::stdout())).context("terminal init failed")?;
    let events = crossterm::event::EventStream::new();
    let result = event_loop(gateway, settings, &mut terminal, events).await;
    drop(guard);
    result
}

/// The loop itself, backend- and input-agnostic so tests can drive it.
async fn event_loop<G: Gateway, B: ratatui::backend::Backend>(
    gateway: G,
    settings: UiSettings,
    terminal: &mut Terminal<B>,
    mut terminal_events: impl futures::Stream<Item = std::io::Result<Event>> + Unpin,
) -> Result<()> {
    let (message_tx, mut message_rx) = mpsc::unbounded_channel::<Msg>();
    let (poll_tx, poll_rx) = mpsc::unbounded_channel::<PollControl>();
    tokio::spawn(poll::run(
        gateway.clone(),
        poll_rx,
        message_tx.clone(),
        settings.refresh_secs,
    ));
    let dispatcher = Dispatcher::new(
        gateway.clone(),
        message_tx.clone(),
        poll_tx.clone(),
        settings.download_dir.clone(),
        std::path::PathBuf::from("samply"),
        crate::browser::detect_browser(),
        crate::browser::detect_revealer(),
    );

    let mut app = App::new(gateway.host(), settings.refresh_secs);
    app.ask_before_destructive = settings.ask_before_destructive;
    app.now_millis = poll::now_millis();
    let mut ticks = tokio::time::interval(std::time::Duration::from_millis(TICK_MILLIS));
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        terminal
            .draw(|frame| crate::ui::render(frame, &app))
            .context("terminal draw failed")?;

        let mut quit = false;
        tokio::select! {
            event = terminal_events.next() => {
                match event {
                    Some(Ok(Event::Key(key))) => {
                        if is_quit_event(&key) {
                            quit = true;
                        } else if let Some(press) = key_press(key) {
                            let commands = update(&mut app, Msg::Key(press));
                            quit = dispatcher.dispatch_all(commands);
                        }
                    }
                    Some(Ok(Event::Mouse(mouse))) => {
                        if let Some(press) = mouse_press(mouse) {
                            let commands = update(&mut app, Msg::Key(press));
                            quit = dispatcher.dispatch_all(commands);
                        }
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => return Err(error).context("terminal event stream failed"),
                    None => quit = true,
                }
            }
            message = message_rx.recv() => {
                if let Some(message) = message {
                    let commands = update(&mut app, message);
                    quit = dispatcher.dispatch_all(commands);
                }
            }
            _ = ticks.tick() => {
                let commands = update(&mut app, Msg::Tick { now_millis: poll::now_millis() });
                quit = dispatcher.dispatch_all(commands);
            }
        }
        // Batch whatever else already arrived before redrawing.
        while let Ok(message) = message_rx.try_recv() {
            let commands = update(&mut app, message);
            quit |= dispatcher.dispatch_all(commands);
        }
        if quit {
            let _ = poll_tx.send(PollControl::Shutdown);
            dispatcher.shutdown();
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{download_file_name, is_quit_event, key_press};
    use crate::app::KeyPress;
    use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

    #[test]
    fn key_mapping_covers_navigation_and_text() {
        let cases = [
            (KeyCode::Up, KeyPress::Up),
            (KeyCode::Down, KeyPress::Down),
            (KeyCode::PageUp, KeyPress::PageUp),
            (KeyCode::PageDown, KeyPress::PageDown),
            (KeyCode::Home, KeyPress::Home),
            (KeyCode::End, KeyPress::End),
            (KeyCode::Enter, KeyPress::Enter),
            (KeyCode::Esc, KeyPress::Escape),
            (KeyCode::Backspace, KeyPress::Backspace),
            (KeyCode::Tab, KeyPress::Tab),
            (KeyCode::BackTab, KeyPress::BackTab),
            (KeyCode::Char('x'), KeyPress::Char('x')),
        ];
        for (code, expected) in cases {
            assert_eq!(
                key_press(KeyEvent::new(code, KeyModifiers::NONE)),
                Some(expected)
            );
        }
        assert_eq!(
            key_press(KeyEvent::new(KeyCode::F(1), KeyModifiers::NONE)),
            None
        );
    }

    #[test]
    fn shift_arrows_map_to_range_selection_keys() {
        assert_eq!(
            key_press(KeyEvent::new(KeyCode::Down, KeyModifiers::SHIFT)),
            Some(KeyPress::ShiftDown)
        );
        assert_eq!(
            key_press(KeyEvent::new(KeyCode::Up, KeyModifiers::SHIFT)),
            Some(KeyPress::ShiftUp)
        );
        // Shift on other keys falls through to the plain mapping.
        assert_eq!(
            key_press(KeyEvent::new(KeyCode::Char('X'), KeyModifiers::SHIFT)),
            Some(KeyPress::Char('X'))
        );
    }

    #[test]
    fn key_releases_are_ignored() {
        let mut release = KeyEvent::new(KeyCode::Char('a'), KeyModifiers::NONE);
        release.kind = KeyEventKind::Release;
        assert_eq!(key_press(release), None);
    }

    #[test]
    fn mouse_wheel_maps_to_scroll_keys_and_the_rest_is_ignored() {
        use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
        let mouse = |kind| MouseEvent {
            kind,
            column: 0,
            row: 0,
            modifiers: KeyModifiers::NONE,
        };
        assert_eq!(
            super::mouse_press(mouse(MouseEventKind::ScrollUp)),
            Some(KeyPress::ScrollUp)
        );
        assert_eq!(
            super::mouse_press(mouse(MouseEventKind::ScrollDown)),
            Some(KeyPress::ScrollDown)
        );
        assert_eq!(
            super::mouse_press(mouse(MouseEventKind::Down(MouseButton::Left))),
            Some(KeyPress::Click { row: 0, column: 0 })
        );
        assert_eq!(
            super::mouse_press(mouse(MouseEventKind::Down(MouseButton::Right))),
            None
        );
    }

    #[test]
    fn ctrl_c_always_quits() {
        assert!(is_quit_event(&KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::CONTROL
        )));
        assert!(!is_quit_event(&KeyEvent::new(
            KeyCode::Char('c'),
            KeyModifiers::NONE
        )));
    }

    #[test]
    fn download_file_names_are_shell_safe() {
        use crate::gateway::DownloadKind;
        assert_eq!(
            download_file_name(DownloadKind::SamplyProfile, "my pipe/../x", 42),
            "my_pipe____x-samply-42.json.gz"
        );
        assert_eq!(
            download_file_name(DownloadKind::SupportBundle, "my pipe/../x", 42),
            "my_pipe____x-support-bundle-42.zip"
        );
    }

    mod dispatcher {
        use super::super::Dispatcher;
        use crate::app::{Cmd, Msg};
        use crate::browser::{Browser, Revealer};
        use crate::gateway::{Action, BundleOptions, DownloadKind, Gateway};
        use crate::http::Download;
        use crate::poll::PollControl;
        use crate::testutil::FakeGateway;
        use tokio::sync::mpsc;

        fn dispatcher_with(
            output_dir: std::path::PathBuf,
            samply_binary: std::path::PathBuf,
            browser: Browser,
            revealer: Revealer,
        ) -> (
            Dispatcher<FakeGateway>,
            FakeGateway,
            mpsc::UnboundedReceiver<Msg>,
            mpsc::UnboundedReceiver<PollControl>,
        ) {
            let gateway = FakeGateway::default();
            let (message_tx, message_rx) = mpsc::unbounded_channel();
            let (poll_tx, poll_rx) = mpsc::unbounded_channel();
            let dispatcher = Dispatcher::new(
                gateway.clone(),
                message_tx,
                poll_tx,
                output_dir,
                samply_binary,
                browser,
                revealer,
            );
            (dispatcher, gateway, message_rx, poll_rx)
        }

        fn no_revealer() -> Revealer {
            Revealer {
                label: "no file manager".to_string(),
                command: vec!["/nonexistent/f5a-no-revealer".to_string()],
                takes_directory: false,
            }
        }

        /// A file manager stand-in that records the path it was handed.
        fn recording_revealer(recorded: &std::path::Path) -> Revealer {
            Revealer {
                label: "the fake file manager".to_string(),
                command: vec![
                    fake_viewer("revealer", &format!("echo \"$@\" > {}", recorded.display()))
                        .display()
                        .to_string(),
                ],
                takes_directory: false,
            }
        }

        #[tokio::test]
        async fn revealing_a_file_runs_the_file_manager_with_its_path() {
            let recorded = scratch_file("revealed-path");
            let (dispatcher, _gateway, mut messages, _poll_rx) = dispatcher_with(
                std::env::temp_dir(),
                std::path::PathBuf::from("/nonexistent/f5a-no-samply"),
                no_browser(),
                recording_revealer(&recorded),
            );
            dispatcher.dispatch(Cmd::RevealFile {
                path: "/tmp/p-support-bundle-1.zip".to_string(),
            });
            let Msg::FileRevealed { path, result } = next(&mut messages).await else {
                panic!("expected a reveal result");
            };
            assert_eq!(path, "/tmp/p-support-bundle-1.zip");
            result.unwrap();
            let mut shown = String::new();
            for _ in 0..50 {
                shown = std::fs::read_to_string(&recorded).unwrap_or_default();
                if !shown.is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            assert_eq!(shown.trim(), "/tmp/p-support-bundle-1.zip");
            let _ = std::fs::remove_file(&recorded);

            let (dispatcher, _gateway, mut messages, _poll_rx) =
                dispatcher_with_dir(std::env::temp_dir());
            dispatcher.dispatch(Cmd::RevealFile {
                path: "x.zip".to_string(),
            });
            let Msg::FileRevealed { result, .. } = next(&mut messages).await else {
                panic!("expected a reveal result");
            };
            assert!(result.unwrap_err().contains("could not start"));
        }

        /// A browser that records the URL it was asked to open in `recorded`.
        fn recording_browser(recorded: &std::path::Path) -> Browser {
            Browser {
                label: "the fake browser".to_string(),
                command: vec![
                    fake_viewer("browser", &format!("echo \"$@\" > {}", recorded.display()))
                        .display()
                        .to_string(),
                ],
            }
        }

        fn no_browser() -> Browser {
            Browser {
                label: "no browser".to_string(),
                command: vec!["/nonexistent/f5a-no-browser".to_string()],
            }
        }

        fn dispatcher_with_dir(
            output_dir: std::path::PathBuf,
        ) -> (
            Dispatcher<FakeGateway>,
            FakeGateway,
            mpsc::UnboundedReceiver<Msg>,
            mpsc::UnboundedReceiver<PollControl>,
        ) {
            dispatcher_with(
                output_dir,
                std::path::PathBuf::from("/nonexistent/f5a-no-samply"),
                no_browser(),
                no_revealer(),
            )
        }

        /// An executable standing in for `samply`, running `body` as sh.
        fn fake_viewer(name: &str, body: &str) -> std::path::PathBuf {
            use std::os::unix::fs::PermissionsExt;
            let path =
                std::env::temp_dir().join(format!("f5a-fake-samply-{}-{name}", std::process::id()));
            std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            path
        }

        async fn open_result(
            viewer: std::path::PathBuf,
            browser: Browser,
        ) -> Result<String, String> {
            let (dispatcher, _gateway, mut messages, _poll_rx) =
                dispatcher_with(std::env::temp_dir(), viewer, browser, no_revealer());
            dispatcher.dispatch(Cmd::OpenSamply {
                path: "profile.json.gz".to_string(),
            });
            let Msg::SamplyOpened { path, result } = next(&mut messages).await else {
                panic!("expected an open result");
            };
            assert_eq!(path, "profile.json.gz");
            result
        }

        /// What samply prints with `--no-open`: the profiler URL on stdout.
        const PROFILER_URL: &str = "https://profiler.firefox.com/from-url/http%3A%2F%2F127.0.0.1%3A3000%2Fx%2Fprofile.json/";
        const SERVING: &str = "echo https://profiler.firefox.com/from-url/http%3A%2F%2F127.0.0.1%3A3000%2Fx%2Fprofile.json/; sleep 4";

        /// A viewer that logs each start to `starts`, its pid to `pid_file`,
        /// prints the URL, and serves until killed.
        fn serving_viewer(
            name: &str,
            starts: &std::path::Path,
            pid_file: &std::path::Path,
            body_after_url: &str,
        ) -> std::path::PathBuf {
            fake_viewer(
                name,
                &format!(
                    "echo started >> {}; echo $$ > {}; echo {PROFILER_URL}; {body_after_url}",
                    starts.display(),
                    pid_file.display()
                ),
            )
        }

        /// Whether the process is gone, or a zombie awaiting its reaper.
        async fn wait_until_gone(pid: &str) -> bool {
            for _ in 0..60 {
                let output = std::process::Command::new("ps")
                    .args(["-o", "stat=", "-p", pid])
                    .output()
                    .unwrap();
                let state = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if !output.status.success() || state.is_empty() || state.starts_with('Z') {
                    return true;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            false
        }

        fn scratch_file(kind: &str) -> std::path::PathBuf {
            std::env::temp_dir().join(format!(
                "f5a-{kind}-{}-{}",
                std::process::id(),
                crate::poll::now_millis()
            ))
        }

        #[tokio::test]
        async fn a_second_open_reuses_the_server_and_shutdown_kills_it() {
            let starts = scratch_file("viewer-starts");
            let pid_file = scratch_file("viewer-pid");
            let recorded = scratch_file("opened-url");
            let (dispatcher, _gateway, mut messages, _poll_rx) = dispatcher_with(
                std::env::temp_dir(),
                serving_viewer("reused", &starts, &pid_file, "exec sleep 30"),
                recording_browser(&recorded),
                no_revealer(),
            );
            for _ in 0..2 {
                dispatcher.dispatch(Cmd::OpenSamply {
                    path: "profile.json.gz".to_string(),
                });
                let Msg::SamplyOpened { result, .. } = next(&mut messages).await else {
                    panic!("expected an open result");
                };
                assert_eq!(result.unwrap(), "the fake browser");
            }
            assert_eq!(
                std::fs::read_to_string(&starts).unwrap().lines().count(),
                1,
                "one server per profile"
            );
            let pid = std::fs::read_to_string(&pid_file)
                .unwrap()
                .trim()
                .to_string();
            dispatcher.shutdown();
            assert!(
                wait_until_gone(&pid).await,
                "viewer {pid} still alive after shutdown"
            );
            for file in [&starts, &pid_file, &recorded] {
                let _ = std::fs::remove_file(file);
            }
        }

        #[tokio::test]
        async fn a_dead_server_is_replaced_on_the_next_open() {
            let starts = scratch_file("dead-viewer-starts");
            let pid_file = scratch_file("dead-viewer-pid");
            let (dispatcher, _gateway, mut messages, _poll_rx) = dispatcher_with(
                std::env::temp_dir(),
                serving_viewer("dies", &starts, &pid_file, "exit 0"),
                no_browser(),
                no_revealer(),
            );
            for _ in 0..2 {
                dispatcher.dispatch(Cmd::OpenSamply {
                    path: "profile.json.gz".to_string(),
                });
                let Msg::SamplyOpened { result, .. } = next(&mut messages).await else {
                    panic!("expected an open result");
                };
                assert!(result.is_err(), "the fake browser cannot start");
                // Let the short-lived server finish exiting before the retry.
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
            assert_eq!(
                std::fs::read_to_string(&starts).unwrap().lines().count(),
                2,
                "a dead server is not reused"
            );
            for file in [&starts, &pid_file] {
                let _ = std::fs::remove_file(file);
            }
        }

        #[tokio::test]
        async fn opening_a_profile_reports_a_missing_viewer() {
            let error = open_result(
                std::path::PathBuf::from("/nonexistent/f5a-no-samply"),
                no_browser(),
            )
            .await
            .unwrap_err();
            assert!(error.contains("could not start"), "{error}");
        }

        #[tokio::test]
        async fn opening_a_profile_hands_the_url_to_the_chosen_browser() {
            let recorded = std::env::temp_dir().join(format!(
                "f5a-opened-url-{}-{}",
                std::process::id(),
                crate::poll::now_millis()
            ));
            let browser = open_result(fake_viewer("serves", SERVING), recording_browser(&recorded))
                .await
                .unwrap();
            assert_eq!(browser, "the fake browser");
            // The browser is spawned detached; give it a moment to write.
            let mut opened = String::new();
            for _ in 0..50 {
                opened = std::fs::read_to_string(&recorded).unwrap_or_default();
                if !opened.is_empty() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            assert!(
                opened.starts_with("https://profiler.firefox.com/from-url/"),
                "{opened}"
            );
            let _ = std::fs::remove_file(&recorded);
        }

        #[tokio::test]
        async fn opening_a_profile_reports_a_browser_that_cannot_start() {
            let error = open_result(fake_viewer("serves-no-browser", SERVING), no_browser())
                .await
                .unwrap_err();
            assert!(error.contains("no browser"), "{error}");
            assert!(
                error.contains("open https://profiler.firefox.com"),
                "{error}"
            );
        }

        #[tokio::test]
        async fn opening_a_profile_reports_a_viewer_that_quits() {
            let error = open_result(fake_viewer("quits", "exit 3"), no_browser())
                .await
                .unwrap_err();
            assert!(error.contains("exited early"), "{error}");
        }

        fn dispatcher() -> (
            Dispatcher<FakeGateway>,
            FakeGateway,
            mpsc::UnboundedReceiver<Msg>,
            mpsc::UnboundedReceiver<PollControl>,
        ) {
            dispatcher_with_dir(std::env::temp_dir())
        }

        async fn next(messages: &mut mpsc::UnboundedReceiver<Msg>) -> Msg {
            tokio::time::timeout(std::time::Duration::from_secs(5), messages.recv())
                .await
                .expect("message expected")
                .expect("channel open")
        }

        #[tokio::test]
        async fn poller_commands_are_forwarded() {
            let (dispatcher, _gateway, _messages, mut poll_rx) = dispatcher();
            assert!(!dispatcher.dispatch(Cmd::Refresh));
            assert!(!dispatcher.dispatch(Cmd::Watch(Some("p".to_string()))));
            assert!(!dispatcher.dispatch(Cmd::SetRefreshSecs(5)));
            assert_eq!(poll_rx.recv().await, Some(PollControl::RefreshNow));
            assert_eq!(
                poll_rx.recv().await,
                Some(PollControl::Watch(Some("p".to_string())))
            );
            assert_eq!(poll_rx.recv().await, Some(PollControl::SetRefreshSecs(5)));
        }

        #[tokio::test]
        async fn actions_round_trip_through_the_gateway() {
            let (dispatcher, gateway, mut messages, _poll_rx) = dispatcher();
            let action = Action::Start {
                pipeline: "p".to_string(),
            };
            dispatcher.dispatch(Cmd::Run(action.clone()));
            let Msg::ActionFinished {
                action: finished,
                result,
            } = next(&mut messages).await
            else {
                panic!("expected an action result");
            };
            assert_eq!(finished, action);
            result.unwrap();
            assert_eq!(gateway.state.lock().unwrap().executed_actions, vec![action]);
        }

        #[tokio::test]
        async fn download_fetches_come_back_as_messages() {
            let (dispatcher, gateway, mut messages, _poll_rx) = dispatcher();
            gateway
                .state
                .lock()
                .unwrap()
                .downloads
                .push_back(Ok(Download::Pending {
                    retry_after_secs: 3,
                }));
            dispatcher.dispatch(Cmd::FetchDownload {
                kind: DownloadKind::SamplyProfile,
                pipeline: "p".to_string(),
                bundle: BundleOptions::default(),
            });
            let Msg::DownloadFetched {
                kind,
                pipeline,
                result,
            } = next(&mut messages).await
            else {
                panic!("expected a samply answer");
            };
            assert_eq!(kind, DownloadKind::SamplyProfile);
            assert_eq!(pipeline, "p");
            assert_eq!(
                result.unwrap(),
                Download::Pending {
                    retry_after_secs: 3
                }
            );
            assert_eq!(
                gateway.state.lock().unwrap().download_requests,
                vec![(DownloadKind::SamplyProfile, "p".to_string())]
            );
        }

        #[tokio::test]
        async fn sql_and_hotspot_fetches_come_back_as_messages() {
            let (dispatcher, gateway, mut messages, _poll_rx) = dispatcher();
            gateway
                .state
                .lock()
                .unwrap()
                .sql
                .push_back(Ok("SELECT 1;".to_string()));
            dispatcher.dispatch(Cmd::FetchSql("p".to_string()));
            let Msg::SqlLoaded { pipeline, result } = next(&mut messages).await else {
                panic!("expected SQL");
            };
            assert_eq!(pipeline, "p");
            assert_eq!(result.unwrap(), "SELECT 1;");

            dispatcher.dispatch(Cmd::FetchHotspots("p".to_string()));
            let Msg::HotspotsLoaded { result, .. } = next(&mut messages).await else {
                panic!("expected hotspots");
            };
            assert!(result.is_err(), "fake queue is empty");
        }

        #[tokio::test]
        async fn samply_saves_land_on_disk() {
            // The directory does not exist yet: saving creates it.
            let output_dir = std::env::temp_dir().join(format!(
                "f5a-test-{}-{}",
                std::process::id(),
                crate::poll::now_millis()
            ));
            let (dispatcher, _gateway, mut messages, _poll_rx) =
                dispatcher_with_dir(output_dir.clone());
            dispatcher.dispatch(Cmd::SaveDownload {
                kind: DownloadKind::SamplyProfile,
                pipeline: "p".to_string(),
                bytes: vec![1, 2, 3],
            });
            let Msg::FileSaved {
                kind: _,
                pipeline,
                path,
                result,
            } = next(&mut messages).await
            else {
                panic!("expected a save confirmation");
            };
            result.unwrap();
            assert_eq!(pipeline, "p");
            assert!(path.ends_with(".json.gz"));
            assert_eq!(std::fs::read(&path).unwrap(), vec![1, 2, 3]);
            let _ = std::fs::remove_dir_all(&output_dir);
        }

        #[tokio::test]
        async fn failed_saves_report_the_error() {
            // A regular file where a directory is needed cannot be created.
            let blocker = std::env::temp_dir().join(format!(
                "f5a-not-a-dir-{}-{}",
                std::process::id(),
                crate::poll::now_millis()
            ));
            std::fs::write(&blocker, b"x").unwrap();
            let (dispatcher, _gateway, mut messages, _poll_rx) =
                dispatcher_with_dir(blocker.join("profiles"));
            dispatcher.dispatch(Cmd::SaveDownload {
                kind: DownloadKind::SamplyProfile,
                pipeline: "p".to_string(),
                bytes: vec![1],
            });
            let Msg::FileSaved { result, .. } = next(&mut messages).await else {
                panic!("expected a save result");
            };
            assert!(result.is_err());
            let _ = std::fs::remove_file(&blocker);
        }

        #[tokio::test]
        async fn log_streams_pump_lines_then_report_closure() {
            let (dispatcher, gateway, mut messages, _poll_rx) = dispatcher();
            gateway.state.lock().unwrap().log_lines = vec!["alpha".to_string(), "beta".to_string()];
            dispatcher.dispatch(Cmd::StartLogs("p".to_string()));
            let Msg::LogLine { pipeline, line } = next(&mut messages).await else {
                panic!("expected a log line");
            };
            assert_eq!(pipeline, "p");
            assert_eq!(line, "alpha");
            let Msg::LogLine { line, .. } = next(&mut messages).await else {
                panic!("expected a log line");
            };
            assert_eq!(line, "beta");
            let Msg::LogsClosed { pipeline, result } = next(&mut messages).await else {
                panic!("expected stream closure");
            };
            assert_eq!(pipeline, "p");
            result.unwrap();
            assert_eq!(
                gateway.state.lock().unwrap().log_stream_requests,
                vec!["p".to_string()]
            );
            // Stopping with no active stream is harmless.
            dispatcher.dispatch(Cmd::StopLogs);
            dispatcher.dispatch(Cmd::StopLogs);
        }

        #[tokio::test]
        async fn diagnostics_fetches_come_back_as_messages() {
            let (dispatcher, _gateway, mut messages, _poll_rx) = dispatcher();
            dispatcher.dispatch(Cmd::FetchDiagnostics("p".to_string()));
            let Msg::DiagnosticsLoaded { pipeline, result } = next(&mut messages).await else {
                panic!("expected diagnostics");
            };
            assert_eq!(pipeline, "p");
            assert!(result.unwrap().is_empty(), "fake default is empty");
        }

        #[tokio::test]
        async fn tenant_switch_and_quit_are_synchronous() {
            let (dispatcher, gateway, _messages, _poll_rx) = dispatcher();
            assert!(!dispatcher.dispatch(Cmd::SwitchTenant(Some("acme".to_string()))));
            assert_eq!(gateway.tenant().as_deref(), Some("acme"));
            assert!(dispatcher.dispatch(Cmd::Quit));
            assert!(dispatcher.dispatch_all(vec![Cmd::Refresh, Cmd::Quit]));
            assert!(!dispatcher.dispatch_all(vec![]));
        }
    }

    mod event_loop {
        use super::super::event_loop;
        use crate::testutil::{FakeGateway, overview_with};
        use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
        use futures::SinkExt;
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        fn key(code: KeyCode) -> std::io::Result<Event> {
            Ok(Event::Key(KeyEvent::new(code, KeyModifiers::NONE)))
        }

        fn test_settings() -> crate::options::UiSettings {
            crate::options::UiSettings {
                refresh_secs: 1,
                ask_before_destructive: false,
                download_dir: std::env::temp_dir(),
            }
        }

        async fn run_with_events(
            gateway: FakeGateway,
            scripted: Vec<std::io::Result<Event>>,
        ) -> Terminal<TestBackend> {
            let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
            let (mut event_tx, event_rx) = futures::channel::mpsc::unbounded();
            tokio::spawn(async move {
                for event in scripted {
                    // Let a frame render between events.
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    if event_tx.send(event).await.is_err() {
                        return;
                    }
                }
            });
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                event_loop(gateway, test_settings(), &mut terminal, event_rx),
            )
            .await
            .expect("the loop must quit")
            .expect("the loop must not error");
            terminal
        }

        #[tokio::test(start_paused = true)]
        async fn keys_flow_through_the_reducer_until_quit() {
            let gateway = FakeGateway::default();
            gateway.push_overview(overview_with(&["a", "b"]));
            let terminal = run_with_events(
                gateway.clone(),
                vec![
                    key(KeyCode::Down),
                    Ok(Event::Resize(100, 30)),
                    key(KeyCode::Char('q')),
                ],
            )
            .await;
            // The dashboard rendered the fetched pipelines before quitting.
            let buffer = terminal.backend().buffer();
            let mut text = String::new();
            for y in 0..buffer.area.height {
                for x in 0..buffer.area.width {
                    text.push_str(buffer[(x, y)].symbol());
                }
            }
            assert!(text.contains("PIPELINES"));
            // The Down key moved the watch to the second pipeline.
            let state = gateway.state.lock().unwrap();
            assert!(state.detail_requests.iter().any(|name| name == "b"));
        }

        #[tokio::test(start_paused = true)]
        async fn ctrl_c_quits_immediately() {
            let gateway = FakeGateway::default();
            gateway.push_overview(overview_with(&[]));
            run_with_events(
                gateway,
                vec![Ok(Event::Key(KeyEvent::new(
                    KeyCode::Char('c'),
                    KeyModifiers::CONTROL,
                )))],
            )
            .await;
        }

        #[tokio::test(start_paused = true)]
        async fn a_closed_event_stream_ends_the_loop() {
            let gateway = FakeGateway::default();
            gateway.push_overview(overview_with(&[]));
            run_with_events(gateway, vec![]).await;
        }

        #[tokio::test(start_paused = true)]
        async fn an_event_stream_error_surfaces_as_an_error() {
            let gateway = FakeGateway::default();
            gateway.push_overview(overview_with(&[]));
            let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
            let (mut event_tx, event_rx) = futures::channel::mpsc::unbounded();
            tokio::spawn(async move {
                let _ = event_tx.send(Err(std::io::Error::other("tty gone"))).await;
            });
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                event_loop(gateway, test_settings(), &mut terminal, event_rx),
            )
            .await
            .expect("the loop must end");
            assert!(result.is_err());
        }
    }
}
