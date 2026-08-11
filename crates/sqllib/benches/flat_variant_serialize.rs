//! Serialization throughput for [`FlatVariant`], the flat-buffer SQL VARIANT.
//!
//! A `FlatVariant` archives as its own encoding, so serializing one document is
//! a bulk copy of the document buffer plus a 16-byte `ArchivedVec` header. This
//! benchmark measures that copy at scale: it builds `--docs` documents of
//! `--size` encoded bytes each, then serializes all of them with
//! `DbspSerializer` through each of the two paths the runtime uses.
//!
//! | mode                      | models                                             |
//! |---------------------------|----------------------------------------------------|
//! | fresh `FBuf` per document | `to_bytes`: checkpoints, worker-to-worker payloads  |
//! | shared `FBuf`             | the layer-file writer filling a data block          |
//!
//! The gap between the two is buffer allocation, teardown, and first touch, so
//! the shared-buffer rate is the ceiling the encoding can reach.
//!
//! The default workload is 100,000 documents of 16 KiB, which holds 1.6 GiB of
//! documents live, so expect a little over 2 GiB of RSS.
//!
//! ```text
//! cargo bench -p feldera-sqllib --bench flat_variant_serialize
//! cargo bench -p feldera-sqllib --bench flat_variant_serialize -- --docs 10000 --size 4096
//! ```

use std::env::args;
use std::hint::black_box;
use std::process::exit;
use std::time::{Duration, Instant};

use dbsp::storage::buffer_cache::{FBuf, FBufSerializer};
use dbsp::storage::file::{SerializerInner, to_bytes};
use dbsp::trace::aligned_deserialize;
use feldera_sqllib::FlatVariant;
use rkyv::ser::Serializer as _;
use size_of::SizeOf;

/// Bytes rkyv writes for one document beyond the document itself: the
/// `ArchivedVec<u8>` that points at the copied encoding.
const HEADER_BYTES: usize = size_of::<rkyv::Archived<FlatVariant>>();

/// rkyv aligns each archived value, so a document that is a whole number of
/// these needs no padding between its bytes and its header.
const ALIGNMENT: usize = 8;

fn main() {
    let config = Config::from_args();
    let workload = Workload::calibrate(&config);
    report_configuration(&config, &workload);

    let corpus = generate(&config, &workload);
    report_generation(&corpus);

    let modes = Mode::ALL.map(|mode| (mode, mode.measure(&corpus, &config)));
    report_serialization(&config, &corpus, &modes);

    if config.check {
        check_round_trip(&corpus);
    }
}

// Configuration

struct Config {
    documents: usize,
    /// Encoded bytes per document; a multiple of [`ALIGNMENT`].
    document_bytes: usize,
    passes: usize,
    seed: u64,
    /// Bytes to accumulate in the shared buffer before recycling it.
    flush_bytes: usize,
    check: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            documents: if SMOKE { 200 } else { 100_000 },
            document_bytes: 16 * 1024,
            passes: if SMOKE { 1 } else { 3 },
            seed: 0x5eed_1234_5678_9abc,
            flush_bytes: 1 << 20,
            check: true,
        }
    }
}

/// An unoptimized build cannot produce a meaningful rate, and `cargo test
/// --benches` runs this binary with no arguments, so a build that is not the
/// `bench` or `release` profile does a smoke run by default. `--docs` and
/// `--passes` still override it.
const SMOKE: bool = cfg!(debug_assertions);

impl Config {
    fn from_args() -> Self {
        let mut config = Config::default();
        let mut args = args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--docs" => config.documents = number(&mut args, &arg),
                "--size" => config.document_bytes = number(&mut args, &arg),
                "--passes" => config.passes = number(&mut args, &arg),
                "--seed" => config.seed = number(&mut args, &arg) as u64,
                "--flush" => config.flush_bytes = number(&mut args, &arg),
                "--no-check" => config.check = false,
                // `cargo bench` passes `--bench`, and a test runner may pass
                // `--test` to ask for a smoke run rather than a measurement.
                "--bench" => (),
                "--test" => {
                    config.documents = 200;
                    config.passes = 1;
                }
                "--help" | "-h" => {
                    usage();
                    exit(0);
                }
                other => fail(&format!("unknown argument {other:?}; try --help")),
            }
        }
        if config.documents < 2 {
            fail("--docs must be at least 2");
        }
        if config.passes == 0 {
            fail("--passes must be at least 1");
        }
        if config.flush_bytes < config.document_bytes {
            fail("--flush must be at least one document");
        }
        // Round up rather than reject: an unaligned size is unreachable, and the
        // report prints the size actually used.
        config.document_bytes = config.document_bytes.next_multiple_of(ALIGNMENT);
        config
    }

    fn serialized_bytes(&self) -> usize {
        self.document_bytes + HEADER_BYTES
    }
}

fn number(args: &mut impl Iterator<Item = String>, flag: &str) -> usize {
    let Some(value) = args.next() else {
        fail(&format!("{flag} requires a value"));
    };
    let (digits, radix) = match value.strip_prefix("0x") {
        Some(hex) => (hex, 16),
        None => (value.as_str(), 10),
    };
    match usize::from_str_radix(&digits.replace('_', ""), radix) {
        Ok(number) => number,
        Err(error) => fail(&format!("{flag}: {value:?} is not a number: {error}")),
    }
}

fn usage() {
    println!(
        "FlatVariant serialization benchmark

  --docs N     documents to serialize (default 100000)
  --size N     encoded bytes per document, rounded up to a multiple of 8
               (default 16384)
  --passes N   times to repeat each serialization mode (default 3)
  --seed N     seed for document contents (default 0x5eed123456789abc)
  --flush N    bytes to accumulate in the shared buffer before recycling it
               (default 1048576)
  --no-check   skip the round-trip check
  --test       smoke run: 200 documents, one pass"
    );
}

fn fail(message: &str) -> ! {
    eprintln!("error: {message}");
    exit(2);
}

// Document generation
//
// A generated document imitates a customer telemetry record: a root map with a
// nested session map, a `props` map of sub-maps holding most of the fields, an
// array of event maps, and an array of tags. Field names come from a small
// vocabulary, so keys repeat across documents the way real ones do.
//
// Every string in a document has a fixed length and every number encodes to a
// fixed-width payload, so all documents encode to exactly the same size whatever
// contents the generator picks. `Workload::calibrate` relies on that to hit
// `--size` exactly, and generation verifies it for every document.

/// Field-name vocabulary, so keys read like telemetry rather than `f0001`.
const WORDS: [&str; 12] = [
    "clicks",
    "dwell_ms",
    "last_seen",
    "referrer",
    "variant",
    "platform",
    "app_version",
    "locale",
    "utm_source",
    "churn_risk",
    "impressions",
    "bucket",
];

/// Kind of placeholder a [`Patch`] refills, which fixes the alphabet it draws
/// from and so keeps the template valid JSON.
#[derive(Clone, Copy)]
enum Placeholder {
    /// Characters inside a string literal.
    Text,
    /// Digits of an integer literal, whose first digit must not be zero.
    Integer,
    /// Digits after the `0.` of a fraction, where a leading zero is fine.
    Fraction,
}

/// A region of the JSON template refilled for each document. The region's length
/// never changes, so neither does the encoded document size.
struct Patch {
    at: usize,
    len: usize,
    placeholder: Placeholder,
}

/// The document shape: how many of each repeated element the template holds.
#[derive(Clone, Copy)]
struct Shape {
    /// Sub-maps of `props`, the knob calibration turns to reach `--size`.
    sections: usize,
    fields_per_section: usize,
    events: usize,
    tags: usize,
    /// Length of the `pad` string, which closes the last bytes to `--size`.
    pad: usize,
}

impl Shape {
    /// Root fields: id, ts, active, score, region, session, props, events, tags,
    /// zzz_pad.
    const ROOT_KEYS: usize = 10;
    /// Fields of the nested `session` map.
    const SESSION_KEYS: usize = 6;
    /// Fields of one `events` entry.
    const EVENT_KEYS: usize = 5;

    fn with_sections(sections: usize, pad: usize) -> Self {
        Self {
            sections,
            fields_per_section: 24,
            events: 4,
            tags: 8,
            pad,
        }
    }

    fn keys(&self) -> usize {
        Self::ROOT_KEYS
            + Self::SESSION_KEYS
            + self.sections * (1 + self.fields_per_section)
            + self.events * Self::EVENT_KEYS
    }
}

/// A JSON document with its variable regions marked, reused for every document
/// so that generating one costs a refill and a parse rather than a text build
/// and a parse.
struct Template {
    json: Vec<u8>,
    patches: Vec<Patch>,
}

impl Template {
    fn build(shape: &Shape) -> Self {
        let mut template = Template {
            json: Vec::with_capacity(4096),
            patches: Vec::new(),
        };
        template.raw("{");
        template.key("id");
        template.text(32);
        template.raw(",");
        template.key("ts");
        template.integer(13);
        template.raw(",");
        template.key("active");
        template.raw("true,");
        template.key("score");
        template.fraction(6);
        template.raw(",");
        template.key("region");
        template.text(12);
        template.raw(",");

        template.key("session");
        template.raw("{");
        template.key("start");
        template.integer(13);
        template.raw(",");
        template.key("agent");
        template.text(24);
        template.raw(",");
        template.key("ip");
        template.text(15);
        template.raw(",");
        template.key("referrer");
        template.text(28);
        template.raw(",");
        template.key("depth");
        template.integer(3);
        template.raw(",");
        template.key("bounced");
        template.raw("false},");

        template.key("props");
        template.raw("{");
        for section in 0..shape.sections {
            if section > 0 {
                template.raw(",");
            }
            let name = format!("section_{section:02}");
            template.key(&name);
            template.raw("{");
            for field in 0..shape.fields_per_section {
                if field > 0 {
                    template.raw(",");
                }
                let name = format!("{}_{field:02}", WORDS[(section + field) % WORDS.len()]);
                template.key(&name);
                // The mix of value kinds a telemetry record carries, so the
                // encoding holds variable-width and fixed-width payloads alike.
                match field % 6 {
                    0 => template.text(12),
                    1 => template.integer(10),
                    2 => template.text(24),
                    3 => template.fraction(6),
                    4 => template.raw("true"),
                    _ => template.raw("null"),
                }
            }
            template.raw("}");
        }
        template.raw("},");

        template.key("events");
        template.raw("[");
        for event in 0..shape.events {
            if event > 0 {
                template.raw(",");
            }
            template.raw("{");
            template.key("kind");
            template.text(10);
            template.raw(",");
            template.key("at");
            template.integer(13);
            template.raw(",");
            template.key("ok");
            template.raw("true,");
            template.key("weight");
            template.fraction(6);
            template.raw(",");
            template.key("note");
            template.text(20);
            template.raw("}");
        }
        template.raw("],");

        template.key("tags");
        template.raw("[");
        for tag in 0..shape.tags {
            if tag > 0 {
                template.raw(",");
            }
            template.text(10);
        }
        template.raw("],");

        // Map keys are stored sorted, and this one sorts after every other key,
        // so a document ends in randomized text. A serializer that loses trailing
        // bytes then fails the round-trip check instead of landing on a payload
        // byte that happens to be zero.
        template.key("zzz_pad");
        template.text(shape.pad);
        template.raw("}");
        template
    }

    fn raw(&mut self, text: &str) {
        self.json.extend_from_slice(text.as_bytes());
    }

    fn key(&mut self, name: &str) {
        self.raw("\"");
        self.raw(name);
        self.raw("\":");
    }

    fn text(&mut self, len: usize) {
        self.raw("\"");
        self.placeholder(Placeholder::Text, len);
        self.raw("\"");
    }

    fn integer(&mut self, digits: usize) {
        self.placeholder(Placeholder::Integer, digits);
    }

    fn fraction(&mut self, digits: usize) {
        self.raw("0.");
        self.placeholder(Placeholder::Fraction, digits);
    }

    /// Reserves a variable region, filled with a valid initial value so that the
    /// template parses even before the first refill.
    fn placeholder(&mut self, placeholder: Placeholder, len: usize) {
        if len == 0 {
            return;
        }
        let at = self.json.len();
        match placeholder {
            Placeholder::Text => self.json.resize(at + len, b'a'),
            Placeholder::Integer => {
                self.json.push(b'1');
                self.json.resize(at + len, b'0');
            }
            Placeholder::Fraction => self.json.resize(at + len, b'0'),
        }
        self.patches.push(Patch {
            at,
            len,
            placeholder,
        });
    }

    /// Refills every variable region and parses the result. Structure and sizes
    /// are the template's; only contents change.
    fn document(&mut self, rng: &mut Rng) -> FlatVariant {
        for patch in &self.patches {
            let region = &mut self.json[patch.at..patch.at + patch.len];
            match patch.placeholder {
                Placeholder::Text => fill(region, rng, TEXT_ALPHABET),
                Placeholder::Fraction => fill(region, rng, DIGIT_ALPHABET),
                Placeholder::Integer => {
                    fill(region, rng, DIGIT_ALPHABET);
                    // JSON forbids a leading zero in an integer literal.
                    region[0] = b'1' + (region[0] - b'0') % 9;
                }
            }
        }
        serde_json::from_slice(&self.json).expect("template is valid JSON")
    }
}

/// 32 characters, so one character costs 5 bits of randomness and no division.
const TEXT_ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz-_.012";
/// Eight digits rather than ten, for the same reason.
const DIGIT_ALPHABET: &[u8; 32] = b"01234567012345670123456701234567";

fn fill(region: &mut [u8], rng: &mut Rng, alphabet: &[u8; 32]) {
    // One draw feeds twelve characters, five bits each.
    for chunk in region.chunks_mut(12) {
        let mut bits = rng.next();
        for byte in chunk {
            *byte = alphabet[(bits & 31) as usize];
            bits >>= 5;
        }
    }
}

/// SplitMix64, hand-rolled so that the workload is identical on every machine
/// and across dependency upgrades; the generator needs only uniform bits.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

// Calibration

/// A document shape whose encoded size is exactly `Config::document_bytes`.
struct Workload {
    shape: Shape,
    probes: usize,
}

impl Workload {
    /// Sizes the document shape to `config.document_bytes`.
    ///
    /// Encoded size grows linearly in [`Shape::sections`] and by one byte per
    /// [`Shape::pad`] character, so two probes fix the slope, a division picks
    /// the section count, and the pad closes the remainder. The last walk pushes
    /// the pad to the top of the range that still serializes to the target,
    /// which is where the document needs no alignment padding and its size is
    /// therefore the target exactly.
    fn calibrate(config: &Config) -> Self {
        let target = config.serialized_bytes();
        let mut probes = 0;
        let mut probe = |sections: usize, pad: usize| {
            probes += 1;
            let mut template = Template::build(&Shape::with_sections(sections, pad));
            let document = template.document(&mut Rng(config.seed));
            to_bytes(&document).expect("serialize document").len()
        };

        let one = probe(1, 0);
        let two = probe(2, 0);
        let per_section = two - one;
        let sectionless = one - per_section;
        if sectionless > target {
            fail(&format!(
                "--size {} is too small for this document shape; use at least {}",
                config.document_bytes,
                sectionless - HEADER_BYTES
            ));
        }

        let mut sections = (target - sectionless) / per_section;
        while sections > 0 && probe(sections, 0) > target {
            sections -= 1;
        }
        let mut pad = target - probe(sections, 0);
        while probe(sections, pad + 1) == target {
            pad += 1;
        }

        let serialized = probe(sections, pad);
        assert_eq!(
            serialized, target,
            "calibration missed the target document size"
        );
        Workload {
            shape: Shape::with_sections(sections, pad),
            probes,
        }
    }
}

// Generation

struct Corpus {
    documents: Vec<FlatVariant>,
    /// Bytes one pass over the corpus writes.
    serialized_bytes: usize,
    wall: Duration,
}

/// Builds the documents and checks that each one serializes to the calibrated
/// size. That check doubles as a warm-up: it faults in every document buffer, so
/// the first measured pass is not the one paying for first touch.
fn generate(config: &Config, workload: &Workload) -> Corpus {
    let expected = config.serialized_bytes();
    let mut template = Template::build(&workload.shape);
    let mut rng = Rng(config.seed);
    let mut documents = Vec::with_capacity(config.documents);

    let start = Instant::now();
    for index in 0..config.documents {
        let document = template.document(&mut rng);
        let serialized = to_bytes(&document).expect("serialize document").len();
        assert_eq!(
            serialized, expected,
            "document {index} serialized to {serialized} bytes, expected {expected}"
        );
        documents.push(document);
    }
    let wall = start.elapsed();

    Corpus {
        serialized_bytes: documents.len() * expected,
        documents,
        wall,
    }
}

// Measurement

#[derive(Clone, Copy)]
enum Mode {
    /// One freshly allocated buffer per document, as `to_bytes` does.
    FreshBuffer,
    /// Many documents into one recycled buffer, as the layer-file writer does.
    SharedBuffer,
}

impl Mode {
    const ALL: [Mode; 2] = [Mode::FreshBuffer, Mode::SharedBuffer];

    fn label(&self) -> &'static str {
        match self {
            Mode::FreshBuffer => "fresh FBuf per document",
            Mode::SharedBuffer => "shared FBuf",
        }
    }

    /// Serializes the whole corpus `config.passes` times.
    fn measure(&self, corpus: &Corpus, config: &Config) -> Vec<Duration> {
        (0..config.passes)
            .map(|_| {
                let pass = match self {
                    Mode::FreshBuffer => fresh_buffer(&corpus.documents),
                    Mode::SharedBuffer => shared_buffer(&corpus.documents, config.flush_bytes),
                };
                assert_eq!(
                    pass.bytes,
                    corpus.serialized_bytes,
                    "{} wrote {} bytes, expected {}",
                    self.label(),
                    pass.bytes,
                    corpus.serialized_bytes
                );
                pass.wall
            })
            .collect()
    }
}

struct Pass {
    wall: Duration,
    bytes: usize,
}

fn fresh_buffer(documents: &[FlatVariant]) -> Pass {
    let mut bytes = 0;
    let start = Instant::now();
    for document in documents {
        let buffer = to_bytes(document).expect("serialize document");
        bytes += buffer.len();
        black_box(buffer.as_slice());
    }
    Pass {
        wall: start.elapsed(),
        bytes,
    }
}

fn shared_buffer(documents: &[FlatVariant], flush_bytes: usize) -> Pass {
    let mut inner = SerializerInner::new();
    let mut buffer = FBuf::with_capacity(flush_bytes);
    let mut bytes = 0;

    let start = Instant::now();
    for document in documents {
        let before = buffer.len();
        inner
            .with(FBufSerializer::new(&mut buffer), |serializer| {
                serializer.serialize_value(document)
            })
            .expect("serialize document");
        bytes += buffer.len() - before;
        if buffer.len() >= flush_bytes {
            black_box(buffer.as_slice());
            buffer.clear();
        }
    }
    black_box(buffer.as_slice());
    Pass {
        wall: start.elapsed(),
        bytes,
    }
}

// Checks

/// Round-trips a sample of documents through the archived form, and confirms
/// that the comparison the round trip rests on can tell two documents apart.
fn check_round_trip(corpus: &Corpus) {
    let documents = &corpus.documents;
    let sampled = 16.min(documents.len());
    let stride = documents.len() / sampled;
    for index in (0..sampled).map(|sample| sample * stride) {
        let document = &documents[index];
        let bytes = to_bytes(document).expect("serialize document");
        let restored: FlatVariant = aligned_deserialize(&bytes[..]);
        assert_eq!(&restored, document, "document {index} changed in transit");
        assert_ne!(
            &restored,
            &documents[(index + 1) % documents.len()],
            "documents are indistinguishable, so the round trip proves nothing"
        );
    }
    println!("round trip: {sampled} documents restored and compared equal");
}

// Reporting

fn report_configuration(config: &Config, workload: &Workload) {
    let shape = &workload.shape;
    println!("FlatVariant serialization through DbspSerializer\n");
    if SMOKE {
        println!("built without optimizations: smoke run, the rates below mean nothing\n");
    }
    println!("configuration");
    row("documents", &count(config.documents), "");
    row(
        "document bytes",
        &count(config.document_bytes),
        &bytes(config.document_bytes as f64),
    );
    row(
        "serialized bytes",
        &count(config.serialized_bytes()),
        &format!("document + {HEADER_BYTES} B ArchivedVec header"),
    );
    row(
        "keys per document",
        &count(shape.keys()),
        &format!(
            "{} sections x {} fields, {} events, {} tags, {} B pad",
            shape.sections, shape.fields_per_section, shape.events, shape.tags, shape.pad
        ),
    );
    row(
        "calibration probes",
        &count(workload.probes),
        "documents built to size the shape",
    );
    row("seed", &format!("{:#x}", config.seed), "");
    row("passes", &count(config.passes), "per mode");
    row(
        "shared buffer flush",
        &count(config.flush_bytes),
        "bytes before the buffer is recycled",
    );
    println!();
}

fn report_generation(corpus: &Corpus) {
    let seconds = corpus.wall.as_secs_f64();
    let documents = corpus.documents.len();
    let heap = corpus.documents.size_of().total_bytes();
    println!("generation (JSON parse plus the size check, not measured)");
    row("wall", &format!("{seconds:.3} s"), "");
    row(
        "rate",
        &count((documents as f64 / seconds) as usize),
        &format!(
            "documents/s, {}/s",
            bytes(corpus.serialized_bytes as f64 / seconds)
        ),
    );
    row(
        "document heap",
        &bytes(heap as f64),
        &format!("size_of, {} B/document", count(heap / documents)),
    );
    println!();
}

fn report_serialization(config: &Config, corpus: &Corpus, modes: &[(Mode, Vec<Duration>)]) {
    let mut header = format!("  {:<24}", "mode");
    for pass in 1..=config.passes {
        let label = format!("pass {pass}");
        header.push_str(&format!("{label:>10}"));
    }
    header.push_str(&format!(
        "{:>10}{:>14}{:>14}",
        "median", "documents/s", "bytes/s"
    ));
    println!("serialization");
    println!("{header}");

    for (mode, passes) in modes {
        let mut line = format!("  {:<24}", mode.label());
        for wall in passes {
            line.push_str(&format!("{:>10.3}", wall.as_secs_f64()));
        }
        let median = median_seconds(passes);
        let rate = count((corpus.documents.len() as f64 / median) as usize);
        let throughput = format!("{}/s", bytes(corpus.serialized_bytes as f64 / median));
        line.push_str(&format!("{median:>10.3}{rate:>14}{throughput:>14}"));
        println!("{line}");
    }
    println!();
}

fn median_seconds(passes: &[Duration]) -> f64 {
    let mut seconds: Vec<f64> = passes.iter().map(Duration::as_secs_f64).collect();
    seconds.sort_by(f64::total_cmp);
    seconds[seconds.len() / 2]
}

fn row(label: &str, value: &str, note: &str) {
    if note.is_empty() {
        println!("  {label:<22}{value:>13}");
    } else {
        println!("  {label:<22}{value:>13}  {note}");
    }
}

/// Formats a byte count in the largest unit that keeps it above one.
fn bytes(mut count: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut unit = 0;
    while count >= 1024.0 && unit + 1 < UNITS.len() {
        count /= 1024.0;
        unit += 1;
    }
    format!("{count:.1} {}", UNITS[unit])
}

/// Groups digits so that six- and ten-digit counts stay readable.
fn count(number: usize) -> String {
    let digits = number.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}
