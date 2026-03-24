//! Annotations for Firefox Profiler profiles.
//!
//! [Firefox Profiler] can display user-defined annotations for timespans and
//! events, as well as information about network activity and memory use, but
//! only if the profiler added those annotations.  The popular [samply]
//! profiler, however, has only very limited support for adding these
//! annotations.  This crate provides a way for a process that is being profiled
//! to record its own annotations and then merge them into profiler output in a
//! postprocessing step.
//!
//! # Use
//!
//! This crate can be integrated into an existing profiler workflow.  For
//! example, the [Feldera incremental compute engine] runs `samply` as a
//! subprocess, targeting itself.  While `samply` runs, Feldera uses [Capture],
//! [Span], and [Event] to record annotations.  After `samply` completes,
//! Feldera finishes the capture to obtain [Annotations], applies them, and then
//! passes the postprocessed output to the user.  The annotation step is
//! invisible to the user.
//!
//! Short of this kind of integration, where a process effectively profiles
//! itself, there must be some way to enable capturing and saving profile data.
//! For example, a command-line option or an environment variable could do the
//! trick.  Once the capture is complete, the process needs to somehow save the
//! annotations.
//!
//! # Viewing in Firefox Profiler
//!
//! Spans and events logged by this module show up in the Marker Chart and
//! Marker Table tabs for a given thread. They are linked to particular threads
//! and the profiler will only show them when those threads are selected.
//!
//! Spans and events are enabled only when a profile is running.  They have
//! minimal overhead otherwise.
//!
//! [samply]: https://github.com/mstange/samply?tab=readme-ov-file#samply
//! [Firefox Profiler]: https://profiler.firefox.com/
//! [Feldera incremental compute engine]: https://feldera.com
#![warn(missing_docs)]
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    io::{Cursor, Read},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Instant,
};

use flate2::{
    Compression,
    bufread::{GzDecoder, GzEncoder},
};
use nix::time::{ClockId, clock_gettime};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use size_of::HumanBytes;
use tracing::warn;

#[derive(Copy, Clone, Debug)]
struct Timestamp(
    /// In nanoseconds in terms of `CLOCK_MONOTONIC`.
    i64,
);

impl Timestamp {
    fn now() -> Self {
        let now = clock_gettime(ClockId::CLOCK_MONOTONIC).unwrap();
        Self(now.tv_sec() as i64 * 1_000_000_000 + now.tv_nsec() as i64)
    }
}

impl From<Instant> for Timestamp {
    fn from(value: Instant) -> Self {
        // SAFETY: On Unix, `Instant` is implemented using CLOCK_MONOTONIC,
        // which is the clock that we need to use for the profiler, but the Rust
        // standard library provides no way to get the value out.  We don't want
        // to make assumptions about the layout of [Instant], and in fact it is
        // not defined as libc's struct timespec but different and
        // Rust-specific.  If we just transmute then we get the wrong value.  It
        // seems rather safer to assume that the all-bytes-zeros `Instant` is
        // the origin, and it works OK for now at least.
        //
        // The completely safe alternative would be to make Timestamp public and
        // force clients to always get both a Timestamp and an Instant if they
        // need both, which is wasteful.
        let zero = unsafe { std::mem::zeroed::<Instant>() };
        Self((value - zero).as_nanos() as i64)
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let microseconds = self.0 as f64 / 1_000_000.0;
        microseconds.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let microseconds = f64::deserialize(deserializer)?;
        Ok(Self((microseconds * 1_000_000.0) as i64))
    }
}

impl Debug for Span {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Span")?;
        if let Some(inner) = &self.0 {
            write!(f, "({})", &inner.name)?;
        }
        Ok(())
    }
}

struct SpanInner {
    start: Timestamp,
    category: &'static str,
    name: &'static str,
    tooltip: String,
}

impl SpanInner {
    #[cold]
    fn new(name: &'static str) -> Self {
        Self {
            start: Timestamp::now(),
            category: "Other",
            name,
            tooltip: String::new(),
        }
    }

    #[cold]
    fn record(self, is_span: bool) {
        let marker = Marker {
            start: self.start,
            end: if is_span {
                Timestamp::now()
            } else {
                self.start
            },
            category: self.category,
            name: self.name,
            tooltip: self.tooltip,
        };
        QUEUE.with(|queue| queue.push(marker));
    }
}

/// Annotates a timespan during a [Capture].
///
/// Constructing and dropping a [Span], when marker spans are being captured
/// with [Capture], records the start and end times of the [Span] along with a
/// name, category, and tooltip.
///
/// `Span` is for timespans.  Use [Event] for point-in-time events.
///
/// When [Capture] is not active, `Span` has minimal overhead.
///
/// [samply]: https://github.com/mstange/samply?tab=readme-ov-file#samply
pub struct Span(Option<SpanInner>);

impl Span {
    /// The number of bytes of memory used during capture to record a [Span] or
    /// [Event].
    pub const BYTES: usize = std::mem::size_of::<Marker>();

    /// Constructs a new [Span] with the given name.  When the constructed
    /// span is dropped, it is automatically recorded.
    ///
    /// [Span] does nothing when markers are not being captured.  A span
    /// will be recorded in a profile only if markers were being captured both
    /// when it was created and when it was dropped.
    ///
    /// The name should ordinarily be a short static string indicating what
    /// happens during the span.  The Firefox Profiler's marker chart view shows
    /// all the spans in a thread with the same name and category on a single
    /// horizontal timeline (unless that would cause overlaps).
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self(Capture::is_active().then(|| SpanInner::new(name)))
    }

    /// Adds `category` to this span.
    ///
    /// The Firefox Profiler's marker chart view groups the markers in each
    /// category and labels them with the category name.
    ///
    /// The default category is "Other".
    #[must_use]
    pub fn with_category(mut self, category: &'static str) -> Self {
        if let Some(inner) = &mut self.0 {
            inner.category = category;
        }
        self
    }

    /// Evaluates `tooltip` and adds it to this span.
    ///
    /// The Firefox Profiler shows the given tooltip in the marker chart
    /// timeline (often truncated) and on hover, and as "details" in the marker
    /// table view.
    ///
    /// `tooltip` is only evaluated if capturing is active.
    #[must_use]
    pub fn with_tooltip<F>(mut self, tooltip: F) -> Self
    where
        F: FnOnce() -> String,
    {
        if let Some(inner) = &mut self.0 {
            inner.tooltip = tooltip();
        }
        self
    }

    /// Sets the starting time for this span to `start`.  The default starting
    /// time is when the [Span] was constructed, so this is only useful if
    /// it's easier to create the span just before recording it.
    #[must_use]
    pub fn with_start(mut self, start: Instant) -> Self {
        if let Some(inner) = &mut self.0 {
            inner.start = start.into();
        }
        self
    }

    /// Calls `f` and records the span.  Returns whatever `f` returned.
    pub fn in_scope<F, T>(self, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        f()
    }

    /// Records the span.
    pub fn record(self) {
        // [Drop] records the span.
    }

    /// Consumes the span without recording it.
    pub fn cancel(mut self) {
        let _ = self.0.take();
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        if let Some(inner) = self.0.take() {
            inner.record(true)
        }
    }
}

/// Annotates an event during a [Capture].
///
/// When a [Capture] is running, use this type to record an event along
/// with a name, category, and tooltip.
///
/// An event happens at a point in time; use [Span] to record a timespan.
///
/// When [Capture] is not active, `Event` has minimal overhead.
///
/// [samply]: https://github.com/mstange/samply?tab=readme-ov-file#samply
/// [module documentation]: crate
pub struct Event(Option<SpanInner>);

impl Event {
    /// Constructs a new [Event] with the given name.
    ///
    /// [Event] does nothing when markers are not being captured.
    ///
    /// The name should ordinarily be a short static string indicating what the
    /// event did.  The Firefox Profiler's marker chart view shows all the
    /// events in a thread with the same name and category on a single
    /// horizontal timeline (unless that would cause overlaps).
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        Self(Capture::is_active().then(|| SpanInner::new(name)))
    }

    /// Adds `category` to this event.
    ///
    /// The Firefox Profiler's marker chart view groups the markers in each
    /// category and labels them with the category name.
    ///
    /// The default category is "Other".
    #[must_use]
    pub fn with_category(mut self, category: &'static str) -> Self {
        if let Some(inner) = &mut self.0 {
            inner.category = category;
        }
        self
    }

    /// Evaluates `tooltip` and adds it to this event.
    ///
    /// The Firefox Profiler shows the given tooltip in the marker chart
    /// timeline (often truncated) and on hover, and as "details" in the marker
    /// table view.
    ///
    /// `tooltip` is only evaluated if capturing is active.
    #[must_use]
    pub fn with_tooltip<F>(mut self, tooltip: F) -> Self
    where
        F: FnOnce() -> String,
    {
        if let Some(inner) = &mut self.0 {
            inner.tooltip = tooltip();
        }
        self
    }

    /// Records the event.
    pub fn record(self) {
        if let Some(inner) = self.0 {
            inner.record(false);
        }
    }
}

/// Options for capturing profile annotations.
#[derive(Default, Clone, Debug)]
pub struct CaptureOptions {
    memory_limit: Option<usize>,
}

impl CaptureOptions {
    /// Creates new capture parameters with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a limit on the amount of memory that can be used for recording
    /// captured spans to `memory_limit`, in bytes.  If `memory_limit` is
    /// `None`, then there will be no limit (which is also the default).
    ///
    /// The limit is honored approximately.  Recording a span takes
    /// [Span::BYTES] bytes.
    pub fn with_memory_limit(self, memory_limit: Option<usize>) -> Self {
        Self {
            memory_limit,
            ..self
        }
    }

    /// Starts capturing profile annotations, returning a [Capture] that can be
    /// used to finish or abort captures.  Dropping the [Capture] will also
    /// abort capturing.
    ///
    /// For use in asynchronous contexts.  If a capture is already in progress,
    /// this function will wait for it to complete before starting a new one.
    pub async fn start(self) -> Capture {
        Capture::new(self, CAPTURE_MUTEX.lock().await)
    }

    /// Starts capturing profile annotations, returning a [Capture] that can be
    /// used to finish or abort captures.  Dropping the [Capture] will also
    /// abort capturing.
    ///
    /// For use in blocking contexts.  If a capture is already in progress, this
    /// function will wait for it to complete before starting a new one.
    ///
    /// # Panic
    ///
    /// Panics if called from an asynchronous execution context.
    pub fn blocking_start(self) -> Capture {
        Capture::new(self, CAPTURE_MUTEX.blocking_lock())
    }

    /// Starts capturing profile annotations, returning a [Capture] that can be
    /// used to finish or abort captures.  Dropping the [Capture] will also
    /// abort capturing.
    ///
    /// If a capture is already in progress, this function returns an error
    /// instead of waiting.
    pub fn try_start(self) -> Result<Capture, Self> {
        let guard = match CAPTURE_MUTEX.try_lock() {
            Ok(guard) => guard,
            Err(_) => return Err(self),
        };
        Ok(Capture::new(self, guard))
    }
}

static CAPTURE_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// An in-progress capture of profile annotations.
///
/// To start a capture, use [CaptureOptions::start],
/// [CaptureOptions::blocking_start], or [CaptureOptions::try_start].
///
/// Only one `Capture` may exist at one time.
pub struct Capture {
    _guard: tokio::sync::MutexGuard<'static, ()>,
    block_limit: i64,
}

impl Capture {
    fn new(params: CaptureOptions, guard: tokio::sync::MutexGuard<'static, ()>) -> Self {
        if let Some(memory_limit) = params.memory_limit {
            tracing::info!(
                "marker capture limited to {}",
                HumanBytes::from(memory_limit)
            );
        }
        let block_limit = params.memory_limit.map_or(i64::MAX, |memory_limit| {
            (memory_limit / BYTES_PER_BLOCK) as i64
        });
        FREE_BLOCKS.store(block_limit, Ordering::Relaxed);
        CAPTURING.store(true, Ordering::Release);
        Self {
            block_limit,
            _guard: guard,
        }
    }

    /// Finishes recording profile annotations and returns what was recorded.
    pub fn finish(self) -> Annotations {
        CAPTURING.store(false, Ordering::Release);
        let remaining_blocks = FREE_BLOCKS.load(Ordering::Relaxed);
        if remaining_blocks <= 0 {
            tracing::info!("marker capture exceeded the limit");
        } else {
            let used_bytes = (self.block_limit - remaining_blocks) as usize * BYTES_PER_BLOCK;
            tracing::info!("marker capture used {}", HumanBytes::from(used_bytes));
        }

        let all_threads = ALL_THREAD_MARKERS.lock().unwrap();
        let mut markers = HashMap::new();
        for thread in &*all_threads {
            markers.insert(thread.tid, (thread.name.clone(), thread.queue.take()));
        }
        Annotations(markers)
    }

    /// Aborts recording profile annotations.
    ///
    /// This is equivalent to dropping the `Capture` object.
    pub fn abort(self) {
        tracing::info!("aborting profile annotation capture");
    }

    /// Returns true if a profile annotation capture is ongoing.
    ///
    /// This only reports the status of annotation captures.  It does not
    /// indicate whether `samply` or `perf` or another profiler is currently
    /// capturing profile data for this process (this crate does not provide a
    /// way to do that).
    pub fn is_active() -> bool {
        CAPTURING.load(Ordering::Acquire)
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        // Might already have been done in [Capture::finish] but it doesn't hurt
        // to do it again (since the lock is still held).
        CAPTURING.store(false, Ordering::Release);
    }
}

/// Error returned by [Annotations::apply].
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Error decompressing the profile.
    #[error("Error decompressing profile ")]
    GzDecoderError(#[from] std::io::Error),

    /// Error parsing the profile.
    #[error("Error parsing profile")]
    SerdeError(#[from] serde_json_path_to_error::Error),
}

/// Options for applying annotations.
#[derive(Default, Clone, Debug)]
pub struct AnnotationOptions {
    product: Option<String>,
    os_cpu: Option<String>,
}

impl AnnotationOptions {
    /// Constructs a default set of options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Overrides the product string in the profile.  `None` uses the default,
    /// which is `PID <pid>`.
    ///
    /// The Firefox Profiler prominently displays the product and OS-CPU string
    /// together in the form `<product> - <OS-CPU>`.
    pub fn with_product(self, product: Option<impl Into<String>>) -> Self {
        Self {
            product: product.map(|s| s.into()),
            ..self
        }
    }

    /// Overrides the OS and CPU string in the profile.  `None` uses the
    /// default, which looks like `Ubuntu 24.0.04.4 LTS`.
    ///
    /// The Firefox Profiler prominently displays the product and OS-CPU string
    /// together in the form `<product> - <OS-CPU>`.
    pub fn with_os_cpu(self, os_cpu: Option<impl Into<String>>) -> Self {
        Self {
            os_cpu: os_cpu.map(|s| s.into()),
            ..self
        }
    }
}

/// Profile annotation data.
///
/// Obtained from [Capture::finish].
pub struct Annotations(HashMap<usize, (Option<String>, Blocks)>);

impl Annotations {
    /// Applies these annotations with the given `options` to `profile`, which
    /// must be the `profile.json` output by samply, and returns the annotated
    /// profile.
    ///
    /// The input may be gzipped or already decompressed.  The output will be in
    /// the same form.
    pub fn apply(&self, profile: &[u8], options: AnnotationOptions) -> Result<Vec<u8>, Error> {
        // Decompress `profile` if it starts with the GZIP magic number,
        // otherwise assume it has already been decompressed.
        let mut buffer = Vec::new();
        let json = if profile.starts_with(&[0x1f, 0x8b]) {
            GzDecoder::new(profile).read_to_end(&mut buffer)?;
            &buffer
        } else {
            profile
        };
        let gzip = !buffer.is_empty();

        // Deserialize.
        let mut profile = serde_json_path_to_error::from_slice::<Profile>(json)?;
        if let Some(product) = options.product {
            profile.meta.product = product;
        }
        if let Some(os_cpu) = options.os_cpu {
            profile.meta.os_cpu = os_cpu;
        }
        profile.meta.marker_schema.push(json!({
            "name": "FelderaMarker",
            "display": [
                "marker-chart",
                "marker-table"
            ],
            "chartLabel": "{marker.data.name}",
            "tooltipLabel": "{marker.data.name}",
            "tableLabel": "{marker.data.name}",
            "description": "Marker generated by Feldera.",
            "fields": [
                {
                    "key": "name",
                    "label": "Name",
                    "format": "unique-string"
                }
            ]
        }));
        /// The colors that the profiler accepts for categories (see
        /// https://github.com/firefox-devtools/profiler/blob/main/src/types/profile.ts).
        static CATEGORY_COLORS: [&str; 12] = [
            "purple",
            "green",
            "orange",
            "yellow",
            "lightblue",
            "blue",
            "brown",
            "magenta",
            "red",
            "lightred",
            "darkgrey",
            "grey",
        ];
        let mut categories = profile
            .meta
            .categories
            .iter()
            .enumerate()
            .map(|(index, category)| (category.name.clone(), index))
            .collect::<HashMap<_, _>>();
        for (category, color) in self
            .0
            .values()
            .flat_map(|(_, markers)| markers.iter())
            .map(|marker| marker.category)
            .collect::<HashSet<_>>()
            .into_iter()
            .zip(CATEGORY_COLORS.iter().cycle())
        {
            categories.insert(category.into(), profile.meta.categories.len());
            profile.meta.categories.push(Category {
                color: (*color).into(),
                name: category.into(),
                other: [(String::from("subcategories"), json!(["Other"]))]
                    .into_iter()
                    .collect(),
            });
        }
        for thread in &mut profile.threads {
            if let Some(tid) = &thread.tid
                && let Ok(tid) = tid.parse::<usize>()
                && let Some((name, markers)) = self.0.get(&tid)
            {
                if let Some(name) = name {
                    thread.name = Some(name.clone());
                }
                for marker in markers.iter() {
                    thread.markers.length += 1;
                    thread.markers.category.push(categories[marker.category]);
                    thread.markers.data.push(ProfileMarkerData {
                        type_: Cow::from("FelderaMarker"),
                        name: profile.shared.add_name(&marker.tooltip),
                    });
                    thread.markers.start_time.push(marker.start);
                    thread.markers.end_time.push(marker.end);
                    thread
                        .markers
                        .name
                        .push(profile.shared.add_name(marker.name));
                    thread.markers.phase.push(1);
                }
            }
        }

        // Produce the output, gzipping it if the input was gzipped.
        let output = serde_json::to_vec(&profile).unwrap();
        let output = if gzip {
            let mut gzipped_output = Vec::new();
            GzEncoder::new(Cursor::new(output), Compression::fast())
                .read_to_end(&mut gzipped_output)
                .unwrap();
            gzipped_output
        } else {
            output
        };

        return Ok(output);

        #[derive(Debug, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Profile {
            meta: Meta,
            threads: Vec<Thread>,
            shared: Shared,
            #[serde(flatten)]
            other: HashMap<String, Value>,
        }

        #[derive(Debug, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Meta {
            product: String,
            #[serde(rename = "oscpu")]
            os_cpu: String,
            categories: Vec<Category>,
            marker_schema: Vec<Value>,
            #[serde(flatten)]
            other: HashMap<String, Value>,
        }

        #[derive(Debug, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Category {
            color: String,
            name: String,
            #[serde(flatten)]
            other: HashMap<String, Value>,
        }

        #[derive(Debug, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Thread {
            name: Option<String>,
            #[serde(default)]
            markers: ProfileMarkers,
            tid: Option<String>,
            #[serde(flatten)]
            other: HashMap<String, Value>,
        }

        #[derive(Default, Debug, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ProfileMarkers {
            length: usize,
            category: Vec<usize>,
            data: Vec<ProfileMarkerData>,
            start_time: Vec<Timestamp>,
            end_time: Vec<Timestamp>,
            name: Vec<usize>,
            phase: Vec<usize>,
        }

        #[derive(Default, Debug, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ProfileMarkerData {
            #[serde(rename = "type")]
            type_: Cow<'static, str>,
            name: usize,
        }

        #[derive(Debug, Serialize, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Shared {
            string_array: Vec<String>,
            #[serde(flatten)]
            other: HashMap<String, Value>,
        }

        impl Shared {
            fn add_name(&mut self, name: &str) -> usize {
                let index = self.string_array.len();
                self.string_array.push(name.into());
                index
            }
        }
    }
}

/// Whether capturing is active.
static CAPTURING: AtomicBool = AtomicBool::new(false);

/// A single marker as captured.
struct Marker {
    /// Start time.
    start: Timestamp,
    /// End time.
    end: Timestamp,
    /// Category (used for outer grouping).
    category: &'static str,
    /// Name (used for inner grouping).
    name: &'static str,
    /// Shown on hover.
    tooltip: String,
}

/// Markers for a given thread.
struct ThreadMarkers {
    /// The thread's tid.
    ///
    /// We need this to identify this thread in the profiler file.
    tid: usize,

    /// The thread's name.
    ///
    /// The profiler gets thread names from the kernel, but they are truncated
    /// at 15 bytes.  We supply the full thread name.
    name: Option<String>,

    /// The thread's markers.
    ///
    /// The thread itself records markers by pushing them onto the queue.
    /// [Capture::finish] pops them all off.
    queue: Arc<Queue>,
}

impl ThreadMarkers {
    fn new(queue: Arc<Queue>) -> Self {
        #[cfg(target_os = "linux")]
        let tid = nix::unistd::gettid().as_raw() as usize;
        #[cfg(not(target_os = "linux"))]
        let tid = thread_id::get();

        Self {
            tid,
            name: std::thread::current().name().map(|s| s.into()),
            queue,
        }
    }
}

/// [ThreadMarkers] for every thread that has recorded a marker.
static ALL_THREAD_MARKERS: std::sync::Mutex<Vec<ThreadMarkers>> = std::sync::Mutex::new(Vec::new());

static FREE_BLOCKS: AtomicI64 = AtomicI64::new(0);
const MARKERS_PER_BLOCK: usize = 32;
const BYTES_PER_BLOCK: usize = MARKERS_PER_BLOCK * Span::BYTES;

struct Block(Vec<Marker>);
impl Block {
    fn new(marker: Marker) -> Self {
        let mut markers = Vec::with_capacity(MARKERS_PER_BLOCK);
        markers.push(marker);
        Self(markers)
    }
    fn is_full(&self) -> bool {
        self.0.len() >= self.0.capacity()
    }
    fn push(&mut self, marker: Marker) {
        self.0.push(marker);
    }
}

struct Blocks(Vec<Block>);

impl Default for Blocks {
    fn default() -> Self {
        Self(Vec::with_capacity(32))
    }
}

impl Blocks {
    fn push(&mut self, marker: Marker) {
        if let Some(block) = self.0.last_mut()
            && !block.is_full()
        {
            block.push(marker);
        } else {
            match FREE_BLOCKS.fetch_sub(1, Ordering::Relaxed) {
                1.. => self.0.push(Block::new(marker)),
                0 => warn!("marker capture space exhausted"),
                _ => (),
            }
        }
    }

    fn iter(&self) -> impl Iterator<Item = &Marker> {
        self.0.iter().flat_map(|block| block.0.iter())
    }
}

struct Queue(std::sync::Mutex<Blocks>);

impl Queue {
    fn new() -> Arc<Self> {
        let queue = Arc::new(Self(Default::default()));
        ALL_THREAD_MARKERS
            .lock()
            .unwrap()
            .push(ThreadMarkers::new(queue.clone()));
        queue
    }

    fn push(&self, marker: Marker) {
        self.0.lock().unwrap().push(marker);
    }

    fn take(&self) -> Blocks {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

thread_local! {
    static QUEUE: Arc<Queue> = Queue::new();
}
