//! Runtime support for `samply`.
//!
//! The [samply] profiler allows the process that it is profiling to indicate
//! important runtime spans so that they show up in the profile for the thread
//! or the process.
//!
//! Use [SamplySpan] to emit a span.
//!
//! # Viewing in the Firefox Profiler
//!
//! Spans logged by this module show up in the Marker Chart and Marker Table
//! tabs for a given thread. They are linked to particular threads and the
//! profiler will only show them when those threads are selected.
//!
//! Spans are enabled only when a profile is running.  They have minimal
//! overhead otherwise.
//!
//! [samply]: https://github.com/mstange/samply?tab=readme-ov-file#samply
#![warn(missing_docs)]
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    io::{Cursor, Read},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Instant,
};

use flate2::{
    Compression,
    bufread::{GzDecoder, GzEncoder},
};
use libc::pid_t;
use nix::{
    time::{ClockId, clock_gettime},
    unistd::{Pid, gettid},
};
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

/// A marker span for the [samply] profiler.
///
/// Constructing and dropping a [SamplySpan], when marker spans are being
/// captured with [Markers::capture], records the start and end times of the
/// [SamplySpan] along with a name, category, and tooltip.
///
/// [samply]: https://github.com/mstange/samply?tab=readme-ov-file#samply
/// [module documentation]: crate::samply
pub struct SamplySpan(Option<SpanInner>);

impl Debug for SamplySpan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SamplySpan")?;
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
    fn record(self) {
        let marker = Marker {
            start: self.start,
            end: Timestamp::now(),
            category: self.category,
            name: self.name,
            tooltip: self.tooltip,
        };
        QUEUE.with(|queue| queue.push(marker));
    }
}

impl SamplySpan {
    /// Constructs a new [SamplySpan] with the given name.  When the constructed
    /// span is dropped, it is automatically recorded.
    ///
    /// [SamplySpan] does nothing when markers are not being captured.  A span
    /// will be recorded in a profile only if markers were being captured both
    /// when it was created and when it was dropped.
    ///
    /// The name should ordinarily be a short static string indicating what
    /// happens during the span.  The Firefox Profiler's marker chart view shows
    /// all the spans in a thread with the same name and category on a single
    /// horizontal timeline (unless that would cause overlaps).
    #[must_use]
    pub fn new(name: &'static str) -> Self {
        if ENABLE_MARKERS.load(Ordering::Acquire) {
            Self(Some(SpanInner {
                start: Timestamp::now(),
                category: "Other",
                name,
                tooltip: String::new(),
            }))
        } else {
            Self(None)
        }
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
    /// `tooltip` is only evaluated if samply is running.
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
    /// time is when the [SamplySpan] was constructed, so this is only useful if
    /// it's easier to create the span just before recording it.
    #[must_use]
    pub fn with_start(mut self, start: Instant) -> Self {
        if let Some(inner) = &mut self.0 {
            inner.start = start.into();
        }
        self
    }

    /// Calls `f` and records the span, returning whatever `f` returned.
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
}

impl Drop for SamplySpan {
    fn drop(&mut self) {
        if let Some(inner) = self.0.take() {
            inner.record()
        }
    }
}

/// Profile marker annotation data.
pub struct Markers(HashMap<Pid, (Option<String>, Vec<Vec<Marker>>)>);

impl Markers {
    /// Calls `f` while capturing profile marker annotation data, and returns
    /// the captured data.  If `memory_limit` is supplied, then no more than
    /// approximately that many bytes of memory will be used for markers.
    ///
    /// Only one call to this function can run at a time; any given call will
    /// block others until it completes.
    pub async fn capture<F, E>(memory_limit: Option<usize>, f: F) -> Result<Self, E>
    where
        F: Future<Output = Result<(), E>>,
    {
        static EXCLUSIVE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
        let _guard = EXCLUSIVE.lock().await;

        if let Some(memory_limit) = memory_limit {
            tracing::info!(
                "marker capture limited to {}",
                HumanBytes::from(memory_limit)
            );
        }
        let initial_blocks =
            memory_limit.map_or(i64::MAX, |memory_limit| (memory_limit / BLOCK_BYTES) as i64);
        FREE_BLOCKS.store(initial_blocks, Ordering::Relaxed);
        ENABLE_MARKERS.store(true, Ordering::Release);
        let result = f.await;
        ENABLE_MARKERS.store(false, Ordering::Release);
        let remaining_blocks = FREE_BLOCKS.load(Ordering::Relaxed);
        if remaining_blocks <= 0 {
            tracing::info!("marker capture exceeded the limit");
        } else {
            let used_bytes = (initial_blocks - remaining_blocks) as usize * BLOCK_BYTES;
            tracing::info!("marker capture used {}", HumanBytes::from(used_bytes));
        }

        result?;
        let all_threads = ALL_THREAD_MARKERS.lock().unwrap();
        let mut markers = HashMap::new();
        for thread in &*all_threads {
            markers.insert(thread.tid, (thread.name.clone(), thread.queue.take()));
        }
        Ok(Self(markers))
    }

    /// Annotates `profile`, which must be the gzipped `profile.json.gz` output
    /// by samply, with our annotations, and returns the annotated, gzipped
    /// profile.
    ///
    /// `product` and `os_cpu` can optionally override the values in the
    /// profile.  The Firefox Profiler shows these for identification purposes
    /// as `product - os_cpu`.  If not overridden, the default is something like
    /// `PID 14 - Ubuntu 24.0.04.4 LTS`.
    pub fn annotate_profile(
        &self,
        profile: &[u8],
        product: Option<&str>,
        os_cpu: Option<&str>,
    ) -> anyhow::Result<Vec<u8>> {
        let mut json = Vec::new();
        GzDecoder::new(profile).read_to_end(&mut json)?;
        let mut profile = serde_json_path_to_error::from_slice::<Profile>(&json)?;
        if let Some(product) = product {
            profile.meta.product = product.into();
        }
        if let Some(os_cpu) = os_cpu {
            profile.meta.os_cpu = os_cpu.into();
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
            .flat_map(|(_, markers)| markers.iter().flatten())
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
                && let Ok(tid) = tid.parse::<pid_t>()
                && let Some((name, markers)) = self.0.get(&Pid::from_raw(tid))
            {
                if let Some(name) = name {
                    thread.name = Some(name.clone());
                }
                for marker in markers.iter().flatten() {
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
        let mut output = Vec::new();
        GzEncoder::new(
            Cursor::new(serde_json::to_vec(&profile).unwrap()),
            Compression::fast(),
        )
        .read_to_end(&mut output)
        .unwrap();
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

/// Whether markers are currently being captured.
static ENABLE_MARKERS: AtomicBool = AtomicBool::new(false);

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
    tid: Pid,

    /// The thread's name.
    ///
    /// The profiler gets thread names from the kernel, but they are truncated
    /// at 15 bytes.  We supply the full thread name.
    name: Option<String>,

    /// The thread's markers.
    ///
    /// The thread itself records markers by pushing them onto the queue.
    /// [Markers::annotate_profile] pops them all off.
    queue: Arc<Queue>,
}

impl ThreadMarkers {
    fn new(queue: Arc<Queue>) -> Self {
        Self {
            tid: gettid(),
            name: std::thread::current().name().map(|s| s.into()),
            queue,
        }
    }
}

/// [ThreadMarkers] for every thread that has recorded a marker.
static ALL_THREAD_MARKERS: std::sync::Mutex<Vec<ThreadMarkers>> = std::sync::Mutex::new(Vec::new());

static FREE_BLOCKS: AtomicI64 = AtomicI64::new(0);
const BLOCK_CAPACITY: usize = 32;
const BLOCK_BYTES: usize = BLOCK_CAPACITY * MARKER_BYTES;

/// The size of a captured marker, in bytes, for calculating the memory limit to
/// pass to [Markers::capture].
pub const MARKER_BYTES: usize = std::mem::size_of::<Marker>();

struct Queue(Mutex<Vec<Vec<Marker>>>);

impl Queue {
    fn new() -> Arc<Self> {
        let queue = Arc::new(Self(Mutex::new(Vec::with_capacity(32))));
        ALL_THREAD_MARKERS
            .lock()
            .unwrap()
            .push(ThreadMarkers::new(queue.clone()));
        queue
    }

    fn push(&self, marker: Marker) {
        let mut queue = self.0.lock().unwrap();
        if let Some(block) = queue.last_mut()
            && block.len() < block.capacity()
        {
            block.push(marker);
        } else {
            match FREE_BLOCKS.fetch_sub(1, Ordering::Relaxed) {
                1.. => {
                    let mut block = Vec::with_capacity(BLOCK_CAPACITY);
                    block.push(marker);
                    queue.push(block);
                }
                0 => warn!("marker capture space exhausted"),
                _ => (),
            }
        }
    }

    fn take(&self) -> Vec<Vec<Marker>> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

thread_local! {
    static QUEUE: Arc<Queue> = Queue::new();
}
