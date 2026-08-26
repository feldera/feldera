//! Downloads the server produces on request, one capture per pipeline and
//! kind: a Samply CPU profile from recording to saved file, or a support
//! bundle from request to saved zip.
//!
//! The samply server records asynchronously: `POST /samply_profile` answers
//! 202 as soon as sampling starts, and `GET /samply_profile?latest=true`
//! answers 204 with a `Retry-After` header until the profile exists. The
//! console mirrors that protocol, so a row can show progress without holding
//! a request open. A support bundle skips the recording and starts fetching.

use crate::gateway::{BundleOptions, DownloadKind};
use crate::http::{ApiError, Download};

/// How long the console keeps asking for a profile after sampling ended;
/// symbolication on a large binary can take a while.
const FETCH_DEADLINE_MILLIS: i64 = 5 * 60 * 1_000;

/// How long a finished capture stays on the row before it fades.
pub const FINISHED_TTL_MILLIS: i64 = 5 * 60 * 1_000;

/// Downloads that may break mid-transfer before the capture fails; the
/// server keeps the profile, so asking again costs nothing.
pub const MAX_TRANSPORT_RETRIES: u32 = 3;

/// Pause before a broken download is retried, multiplied by the attempt.
const RETRY_BACKOFF_MILLIS: i64 = 2_000;

/// Where a capture stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapturePhase {
    /// The profiler samples the pipeline until the window closes. `accepted`
    /// turns true once the server confirmed the request, so a short window
    /// cannot fetch before sampling even began.
    Recording {
        started_millis: i64,
        duration_secs: u64,
        accepted: bool,
    },
    /// Sampling ended; the console asks the server for the profile at
    /// `poll_at_millis` (`None` while a request is in flight) until
    /// `deadline_millis`. `transport_failures` counts downloads that broke
    /// mid-transfer and were retried.
    Fetching {
        poll_at_millis: Option<i64>,
        deadline_millis: i64,
        transport_failures: u32,
    },
    Saved {
        path: String,
        finished_millis: i64,
    },
    Failed {
        error: String,
        finished_millis: i64,
    },
}

/// What the reducer must do after a clock tick advanced a capture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TickOutcome {
    Nothing,
    /// Ask the server for the profile now.
    Fetch,
    /// The server never produced a profile; the capture is now failed.
    GaveUp,
}

/// What the reducer must do after the server answered a profile request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DownloadOutcome {
    /// The profile arrived; write it to disk.
    Save(Vec<u8>),
    /// Not ready yet, or a broken download will be retried at the next poll;
    /// `retry` is the attempt number when a download broke, else `None`.
    Wait { retry: Option<u32> },
    /// The capture failed; the phase carries the reason.
    Failed,
}

/// One pipeline's capture.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Capture {
    pub kind: DownloadKind,
    pub pipeline: String,
    /// What a support bundle request gathers; retries reuse it. Profiles
    /// carry the default and ignore it.
    pub bundle: BundleOptions,
    /// When the user asked for it; orders captures in the status line.
    pub requested_millis: i64,
    pub phase: CapturePhase,
}

impl Capture {
    /// A Samply capture whose recording request is about to be sent.
    ///
    /// ```
    /// use f5a::app::capture::{Capture, CapturePhase};
    /// use f5a::gateway::DownloadKind;
    ///
    /// let capture = Capture::record("orders".to_string(), 30, 1_000);
    /// assert_eq!(capture.kind, DownloadKind::SamplyProfile);
    /// assert!(capture.is_active());
    /// assert!(matches!(capture.phase, CapturePhase::Recording { accepted: false, .. }));
    /// ```
    pub fn record(pipeline: String, duration_secs: u64, now_millis: i64) -> Self {
        Self {
            kind: DownloadKind::SamplyProfile,
            pipeline,
            bundle: BundleOptions::default(),
            requested_millis: now_millis,
            phase: CapturePhase::Recording {
                started_millis: now_millis,
                duration_secs,
                accepted: false,
            },
        }
    }

    /// A capture that fetches a file the server produces on request, such
    /// as a support bundle shaped by `bundle`; the first request is about to
    /// be sent.
    ///
    /// ```
    /// use f5a::app::capture::{Capture, CapturePhase};
    /// use f5a::gateway::{BundleOptions, DownloadKind};
    ///
    /// let capture = Capture::download(
    ///     DownloadKind::SupportBundle,
    ///     "orders".to_string(),
    ///     BundleOptions::default(),
    ///     1_000,
    /// );
    /// assert!(matches!(capture.phase, CapturePhase::Fetching { poll_at_millis: None, .. }));
    /// ```
    pub fn download(
        kind: DownloadKind,
        pipeline: String,
        bundle: BundleOptions,
        now_millis: i64,
    ) -> Self {
        Self {
            kind,
            pipeline,
            bundle,
            requested_millis: now_millis,
            phase: CapturePhase::Fetching {
                poll_at_millis: None,
                deadline_millis: now_millis + FETCH_DEADLINE_MILLIS,
                transport_failures: 0,
            },
        }
    }

    /// Whether the capture still has work ahead of it.
    ///
    /// ```
    /// use f5a::app::capture::Capture;
    ///
    /// let mut capture = Capture::record("orders".to_string(), 30, 0);
    /// assert!(capture.is_active());
    /// capture.fail("boom".to_string(), 5);
    /// assert!(!capture.is_active());
    /// ```
    pub fn is_active(&self) -> bool {
        matches!(
            self.phase,
            CapturePhase::Recording { .. } | CapturePhase::Fetching { .. }
        )
    }

    /// When a finished capture stopped, for fading it out; `None` while active.
    ///
    /// ```
    /// use f5a::app::capture::Capture;
    ///
    /// let mut capture = Capture::record("orders".to_string(), 1, 0);
    /// assert_eq!(capture.finished_millis(), None);
    /// capture.saved("./orders-samply-1.json.gz".to_string(), 9);
    /// assert_eq!(capture.finished_millis(), Some(9));
    /// ```
    pub fn finished_millis(&self) -> Option<i64> {
        match &self.phase {
            CapturePhase::Saved {
                finished_millis, ..
            }
            | CapturePhase::Failed {
                finished_millis, ..
            } => Some(*finished_millis),
            _ => None,
        }
    }

    /// The server confirmed that sampling started.
    ///
    /// ```
    /// use f5a::app::capture::{Capture, CapturePhase};
    ///
    /// let mut capture = Capture::record("orders".to_string(), 30, 0);
    /// capture.accept();
    /// assert!(matches!(capture.phase, CapturePhase::Recording { accepted: true, .. }));
    /// ```
    pub fn accept(&mut self) {
        if let CapturePhase::Recording { accepted, .. } = &mut self.phase {
            *accepted = true;
        }
    }

    /// End the capture with an error.
    ///
    /// ```
    /// use f5a::app::capture::{Capture, CapturePhase};
    ///
    /// let mut capture = Capture::record("orders".to_string(), 30, 0);
    /// capture.fail("HTTP 409: already in progress".to_string(), 700);
    /// assert!(matches!(capture.phase, CapturePhase::Failed { finished_millis: 700, .. }));
    /// ```
    pub fn fail(&mut self, error: String, now_millis: i64) {
        self.phase = CapturePhase::Failed {
            error,
            finished_millis: now_millis,
        };
    }

    /// The profile landed on disk.
    ///
    /// ```
    /// use f5a::app::capture::{Capture, CapturePhase};
    ///
    /// let mut capture = Capture::record("orders".to_string(), 1, 0);
    /// capture.saved("./orders-samply-1.json.gz".to_string(), 3);
    /// assert!(matches!(capture.phase, CapturePhase::Saved { .. }));
    /// ```
    pub fn saved(&mut self, path: String, now_millis: i64) {
        self.phase = CapturePhase::Saved {
            path,
            finished_millis: now_millis,
        };
    }

    /// Fraction of the recording window that has passed, clamped to
    /// `0.0..=1.0`; `1.0` once recording is over.
    ///
    /// ```
    /// use f5a::app::capture::Capture;
    ///
    /// let capture = Capture::record("orders".to_string(), 10, 0);
    /// assert_eq!(capture.progress(5_000), 0.5);
    /// assert_eq!(capture.progress(50_000), 1.0);
    /// ```
    pub fn progress(&self, now_millis: i64) -> f64 {
        match &self.phase {
            CapturePhase::Recording {
                started_millis,
                duration_secs,
                ..
            } => {
                let window_millis = (*duration_secs as f64) * 1_000.0;
                (now_millis.saturating_sub(*started_millis) as f64 / window_millis).clamp(0.0, 1.0)
            }
            _ => 1.0,
        }
    }

    /// Advance the capture by the clock and say what the reducer must do.
    ///
    /// ```
    /// use f5a::app::capture::{Capture, TickOutcome};
    ///
    /// let mut capture = Capture::record("orders".to_string(), 2, 0);
    /// capture.accept();
    /// assert_eq!(capture.tick(1_000), TickOutcome::Nothing);
    /// assert_eq!(capture.tick(2_000), TickOutcome::Fetch);
    /// ```
    pub fn tick(&mut self, now_millis: i64) -> TickOutcome {
        match &mut self.phase {
            CapturePhase::Recording {
                started_millis,
                duration_secs,
                accepted,
            } => {
                let window_over = now_millis - *started_millis >= (*duration_secs * 1_000) as i64;
                if !*accepted || !window_over {
                    return TickOutcome::Nothing;
                }
                self.phase = CapturePhase::Fetching {
                    poll_at_millis: None,
                    deadline_millis: now_millis + FETCH_DEADLINE_MILLIS,
                    transport_failures: 0,
                };
                TickOutcome::Fetch
            }
            CapturePhase::Fetching {
                poll_at_millis,
                deadline_millis,
                ..
            } => {
                if now_millis >= *deadline_millis {
                    self.fail(
                        "the server never produced the profile".to_string(),
                        now_millis,
                    );
                    return TickOutcome::GaveUp;
                }
                match poll_at_millis {
                    Some(poll_at) if now_millis >= *poll_at => {
                        *poll_at_millis = None;
                        TickOutcome::Fetch
                    }
                    _ => TickOutcome::Nothing,
                }
            }
            CapturePhase::Saved { .. } | CapturePhase::Failed { .. } => TickOutcome::Nothing,
        }
    }

    /// Fold in the server's answer to a profile request. A download that
    /// broke mid-transfer is retried a few times with a growing pause, because
    /// the server keeps the profile; a rejection from the server fails the
    /// capture at once.
    ///
    /// ```
    /// use f5a::app::capture::{DownloadOutcome, Capture, CapturePhase};
    /// use f5a::http::Download;
    ///
    /// let mut capture = Capture::record("orders".to_string(), 1, 0);
    /// capture.accept();
    /// capture.tick(1_000);
    /// let pending = Download::Pending { retry_after_secs: 3 };
    /// assert_eq!(capture.on_download(Ok(pending), 1_000), DownloadOutcome::Wait { retry: None });
    /// assert!(matches!(capture.phase, CapturePhase::Fetching { poll_at_millis: Some(4_000), .. }));
    /// assert_eq!(
    ///     capture.on_download(Ok(Download::Ready(vec![1])), 4_000),
    ///     DownloadOutcome::Save(vec![1])
    /// );
    /// ```
    pub fn on_download(
        &mut self,
        result: Result<Download, ApiError>,
        now_millis: i64,
    ) -> DownloadOutcome {
        let CapturePhase::Fetching {
            poll_at_millis,
            transport_failures,
            ..
        } = &mut self.phase
        else {
            return DownloadOutcome::Wait { retry: None };
        };
        match result {
            Ok(Download::Ready(bytes)) => DownloadOutcome::Save(bytes),
            Ok(Download::Pending { retry_after_secs }) => {
                *poll_at_millis = Some(now_millis + (retry_after_secs.max(1) * 1_000) as i64);
                DownloadOutcome::Wait { retry: None }
            }
            Err(error)
                if error.is_connection_loss() && *transport_failures < MAX_TRANSPORT_RETRIES =>
            {
                *transport_failures += 1;
                *poll_at_millis =
                    Some(now_millis + RETRY_BACKOFF_MILLIS * i64::from(*transport_failures));
                DownloadOutcome::Wait {
                    retry: Some(*transport_failures),
                }
            }
            Err(error) => {
                self.fail(error.to_string(), now_millis);
                DownloadOutcome::Failed
            }
        }
    }

    /// Compact badge for the pipeline row: a scrolling waveform with the
    /// countdown while recording, a travelling block while downloading.
    ///
    /// ```
    /// use f5a::app::capture::Capture;
    ///
    /// let capture = Capture::record("orders".to_string(), 30, 0);
    /// assert_eq!(capture.label(15_000), "█▆▄▂ 15s/30s");
    /// assert_eq!(capture.label(15_150), "▆▄▂▁ 15s/30s");
    /// ```
    pub fn label(&self, now_millis: i64) -> String {
        let frame = animation_frame(now_millis);
        match &self.phase {
            CapturePhase::Recording {
                started_millis,
                duration_secs,
                ..
            } => {
                let elapsed_secs = (now_millis.saturating_sub(*started_millis) / 1_000)
                    .clamp(0, *duration_secs as i64);
                format!("{} {elapsed_secs}s/{duration_secs}s", waveform(frame))
            }
            CapturePhase::Fetching { .. } => format!("⇣ {}", marquee(frame)),
            CapturePhase::Saved { .. } => "✓ saved".to_string(),
            CapturePhase::Failed { .. } => "✖ failed".to_string(),
        }
    }

    /// One-line account for the status line, with the saved path or error.
    ///
    /// ```
    /// use f5a::app::capture::Capture;
    ///
    /// let mut capture = Capture::record("orders".to_string(), 1, 0);
    /// capture.saved("./orders-samply-7.json.gz".to_string(), 1);
    /// assert_eq!(
    ///     capture.headline(1),
    ///     "samply `orders`: profile saved to ./orders-samply-7.json.gz"
    /// );
    /// ```
    pub fn headline(&self, now_millis: i64) -> String {
        let subject = format!("{} `{}`", self.kind.tag(), self.pipeline);
        let noun = self.kind.noun();
        match &self.phase {
            CapturePhase::Recording {
                started_millis,
                duration_secs,
                accepted,
            } => {
                if !*accepted {
                    return format!("{subject}: starting the profiler…");
                }
                let elapsed_secs = (now_millis.saturating_sub(*started_millis) / 1_000)
                    .clamp(0, *duration_secs as i64);
                format!(
                    "{subject}: recording {} {elapsed_secs}s/{duration_secs}s",
                    progress_bar(self.progress(now_millis), 20)
                )
            }
            CapturePhase::Fetching {
                transport_failures: 0,
                ..
            } => match self.kind {
                DownloadKind::SamplyProfile => {
                    format!("{subject}: sampling done, fetching the profile…")
                }
                DownloadKind::SupportBundle => {
                    format!("{subject}: collecting the support bundle…")
                }
            },
            CapturePhase::Fetching {
                transport_failures, ..
            } => format!(
                "{subject}: download interrupted, retrying ({transport_failures}/{MAX_TRANSPORT_RETRIES})…"
            ),
            CapturePhase::Saved { path, .. } => format!("{subject}: {noun} saved to {path}"),
            CapturePhase::Failed { error, .. } => format!("{subject}: {error}"),
        }
    }
}

/// Animation cadence of the capture badges; the runner ticks at this rate.
const FRAME_MILLIS: i64 = 150;

/// The animation frame at `now_millis`, shared by the badge text and its
/// color so both move in step.
///
/// ```
/// use f5a::app::capture::animation_frame;
///
/// assert_eq!(animation_frame(0), 0);
/// assert_eq!(animation_frame(450), 3);
/// ```
pub fn animation_frame(now_millis: i64) -> usize {
    (now_millis.max(0) / FRAME_MILLIS) as usize
}

/// One period of the sampling waveform.
const WAVE: [char; 8] = ['▁', '▂', '▄', '▆', '█', '▆', '▄', '▂'];

/// Four cells of the waveform, scrolling one cell per frame.
fn waveform(frame: usize) -> String {
    (0..4)
        .map(|offset| WAVE[(frame + offset) % WAVE.len()])
        .collect()
}

/// A block travelling along four cells: a download of unknown length.
fn marquee(frame: usize) -> String {
    (0..4)
        .map(|cell| if cell == frame % 4 { '▰' } else { '▱' })
        .collect()
}

/// A `width`-cell bar, filled from the left.
fn progress_bar(fraction: f64, width: usize) -> String {
    let filled = ((fraction.clamp(0.0, 1.0) * width as f64).round() as usize).min(width);
    format!("{}{}", "▰".repeat(filled), "▱".repeat(width - filled))
}

#[cfg(test)]
mod tests {
    use super::{
        Capture, CapturePhase, DownloadOutcome, TickOutcome, animation_frame, marquee,
        progress_bar, waveform,
    };
    use crate::http::{ApiError, Download};

    /// A capture whose one-second window closed at `now_millis = 1_000`.
    fn fetching() -> Capture {
        let mut capture = Capture::record("p".to_string(), 1, 0);
        capture.accept();
        assert_eq!(capture.tick(1_000), TickOutcome::Fetch);
        capture
    }

    #[test]
    fn recording_waits_for_acceptance_before_fetching() {
        let mut capture = Capture::record("p".to_string(), 1, 0);
        assert_eq!(
            capture.tick(5_000),
            TickOutcome::Nothing,
            "not accepted yet"
        );
        capture.accept();
        assert_eq!(capture.tick(999), TickOutcome::Nothing);
        assert_eq!(capture.tick(1_000), TickOutcome::Fetch);
        assert!(matches!(
            capture.phase,
            CapturePhase::Fetching {
                poll_at_millis: None,
                ..
            }
        ));
        assert_eq!(
            capture.tick(1_500),
            TickOutcome::Nothing,
            "request in flight"
        );
    }

    #[test]
    fn pending_answers_schedule_the_next_poll() {
        let mut capture = fetching();
        let pending = Download::Pending {
            retry_after_secs: 0,
        };
        assert_eq!(
            capture.on_download(Ok(pending), 1_000),
            DownloadOutcome::Wait { retry: None }
        );
        assert_eq!(capture.tick(1_500), TickOutcome::Nothing);
        assert_eq!(
            capture.tick(2_000),
            TickOutcome::Fetch,
            "zero clamps to one second"
        );
        assert_eq!(
            capture.on_download(Ok(Download::Ready(vec![7])), 2_500),
            DownloadOutcome::Save(vec![7])
        );
        assert!(capture.is_active(), "saving is still ahead");
        capture.saved("out.json.gz".to_string(), 2_600);
        assert!(!capture.is_active());
        assert_eq!(capture.finished_millis(), Some(2_600));
    }

    #[test]
    fn fetch_errors_and_deadlines_fail_the_capture() {
        let mut capture = fetching();
        let error = ApiError::Status {
            status: 500,
            message: "failed to profile the pipeline using samply".to_string(),
        };
        assert_eq!(
            capture.on_download(Err(error), 1_010),
            DownloadOutcome::Failed
        );
        assert!(capture.headline(1_010).contains("failed to profile"));
        assert_eq!(capture.label(1_010), "✖ failed");

        let mut stale = fetching();
        assert_eq!(stale.tick(1_000 + 5 * 60 * 1_000), TickOutcome::GaveUp);
        assert!(matches!(stale.phase, CapturePhase::Failed { .. }));
        assert_eq!(stale.tick(6 * 60 * 1_000), TickOutcome::Nothing);
    }

    #[test]
    fn broken_downloads_are_retried_with_a_growing_pause_then_fail() {
        let interrupted = || ApiError::Transport("download interrupted".to_string());
        let mut capture = fetching();
        assert_eq!(
            capture.on_download(Err(interrupted()), 1_000),
            DownloadOutcome::Wait { retry: Some(1) }
        );
        assert!(capture.headline(1_000).contains("retrying (1/3)"));
        assert_eq!(capture.tick(2_999), TickOutcome::Nothing);
        assert_eq!(capture.tick(3_000), TickOutcome::Fetch, "two seconds later");
        assert_eq!(
            capture.on_download(Err(interrupted()), 3_000),
            DownloadOutcome::Wait { retry: Some(2) }
        );
        assert_eq!(capture.tick(6_999), TickOutcome::Nothing);
        assert_eq!(
            capture.tick(7_000),
            TickOutcome::Fetch,
            "four seconds later"
        );
        assert_eq!(
            capture.on_download(Err(interrupted()), 7_000),
            DownloadOutcome::Wait { retry: Some(3) }
        );
        assert_eq!(
            capture.tick(13_000),
            TickOutcome::Fetch,
            "six seconds later"
        );
        assert_eq!(
            capture.on_download(Err(interrupted()), 13_000),
            DownloadOutcome::Failed,
            "the fourth break is one too many"
        );
        assert!(capture.headline(13_000).contains("download interrupted"));

        // A rejection is never retried, even on the first attempt.
        let mut rejected = fetching();
        let error = ApiError::Status {
            status: 503,
            message: "not deployed".to_string(),
        };
        assert_eq!(
            rejected.on_download(Err(error), 1_000),
            DownloadOutcome::Failed
        );
    }

    #[test]
    fn answers_outside_the_fetching_phase_are_ignored() {
        let mut capture = Capture::record("p".to_string(), 30, 0);
        assert_eq!(
            capture.on_download(Ok(Download::Ready(vec![1])), 100),
            DownloadOutcome::Wait { retry: None }
        );
        assert!(matches!(capture.phase, CapturePhase::Recording { .. }));
    }

    #[test]
    fn labels_and_headlines_follow_the_phase() {
        let mut capture = Capture::record("orders".to_string(), 30, 0);
        assert_eq!(capture.label(0), "▁▂▄▆ 0s/30s");
        assert_eq!(
            capture.headline(0),
            "samply `orders`: starting the profiler…"
        );
        capture.accept();
        assert_eq!(capture.label(30_000), "▁▂▄▆ 30s/30s");
        assert_eq!(
            capture.headline(15_000),
            "samply `orders`: recording ▰▰▰▰▰▰▰▰▰▰▱▱▱▱▱▱▱▱▱▱ 15s/30s"
        );
        capture.tick(30_000);
        assert_eq!(capture.label(30_000), "⇣ ▰▱▱▱");
        assert!(capture.headline(30_000).contains("fetching the profile"));
        capture.saved("./orders-samply-1.json.gz".to_string(), 31_000);
        assert_eq!(capture.label(31_000), "✓ saved");
        assert_eq!(capture.progress(31_000), 1.0);
    }

    #[test]
    fn support_bundles_start_fetching_and_speak_of_bundles() {
        use crate::gateway::{BundleOptions, DownloadKind};
        let mut capture = Capture::download(
            DownloadKind::SupportBundle,
            "p".to_string(),
            BundleOptions::default(),
            0,
        );
        assert!(capture.is_active());
        assert_eq!(capture.label(0), "⇣ ▰▱▱▱");
        assert_eq!(
            capture.headline(0),
            "bundle `p`: collecting the support bundle…"
        );
        assert_eq!(
            capture.on_download(Ok(Download::Ready(vec![1])), 500),
            DownloadOutcome::Save(vec![1])
        );
        capture.saved("./p-support-bundle-1.zip".to_string(), 600);
        assert_eq!(
            capture.headline(600),
            "bundle `p`: support bundle saved to ./p-support-bundle-1.zip"
        );
        assert_eq!(capture.progress(600), 1.0, "no recording window to fill");
    }

    #[test]
    fn badges_animate_frame_by_frame() {
        assert_eq!(waveform(0), "▁▂▄▆");
        assert_eq!(waveform(1), "▂▄▆█");
        assert_eq!(waveform(8), "▁▂▄▆", "the wave wraps");
        assert_eq!(marquee(0), "▰▱▱▱");
        assert_eq!(marquee(2), "▱▱▰▱");
        assert_eq!(marquee(5), "▱▰▱▱");
        assert_eq!(animation_frame(-5), 0, "clocks before the epoch stay put");
        let mut capture = Capture::record("p".to_string(), 30, 0);
        assert_ne!(capture.label(0), capture.label(150));
        capture.accept();
        capture.tick(30_000);
        assert_eq!(capture.label(30_000), "⇣ ▰▱▱▱");
        assert_eq!(capture.label(30_150), "⇣ ▱▰▱▱");
    }

    #[test]
    fn progress_bars_span_the_range() {
        assert_eq!(progress_bar(0.0, 4), "▱▱▱▱");
        assert_eq!(progress_bar(0.5, 4), "▰▰▱▱");
        assert_eq!(progress_bar(2.0, 4), "▰▰▰▰");
    }
}
