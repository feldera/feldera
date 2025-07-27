use itertools::Itertools;
use std::{
    fmt::{Display, Write},
    sync::atomic::{
        AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicU16, AtomicU32, AtomicU64, AtomicU8,
        Ordering,
    },
};

/// A formatter for metrics.
///
/// Don't use this directly but via [MetricsWriter].
pub trait MetricsFormatter {
    fn new() -> Self;
    fn write_description(&mut self, name: &str, help: &str, metric_type: &str);
    fn write_value(&mut self, name: &str, suffix: &str, labels: &LabelStack, value: f64);
    fn write_histogram(&mut self, name: &str, labels: &LabelStack, histogram: &impl Histogram);
    fn end_values(&mut self);
    fn into_output(self) -> String;
}

/// A [MetricsFormatter] that outputs [Prometheus exposition format].
///
/// [Prometheus exposition format]: https://prometheus.io/docs/instrumenting/exposition_formats/
pub struct PrometheusFormatter {
    output: String,
}

impl MetricsFormatter for PrometheusFormatter {
    fn new() -> Self {
        Self {
            output: String::new(),
        }
    }
    fn write_description(&mut self, name: &str, help: &str, metric_type: &str) {
        if !self.output.is_empty() {
            self.output.push('\n');
        }
        if !help.is_empty() {
            writeln!(
                &mut self.output,
                "# HELP {} {}",
                EscapedName(name),
                EscapedHelp(help)
            )
            .unwrap();
        }
        writeln!(
            &mut self.output,
            "# TYPE {} {metric_type}",
            EscapedName(name)
        )
        .unwrap();
    }

    fn write_value(&mut self, name: &str, suffix: &str, labels: &LabelStack, value: f64) {
        write!(&mut self.output, "{}{suffix}", EscapedName(name)).unwrap();
        if !labels.is_empty() {
            self.output.write_char('{').unwrap();
            let mut index = 0;
            labels.iterate(&mut |name, value| {
                if index != 0 {
                    self.output.write_char(',').unwrap();
                }
                index += 1;
                write!(
                    &mut self.output,
                    "{}=\"{}\"",
                    EscapedName(name),
                    EscapedValue(value)
                )
                .unwrap();
            });
            self.output.write_char('}').unwrap();
        }
        writeln!(&mut self.output, " {value}").unwrap();
    }

    fn write_histogram(&mut self, name: &str, labels: &LabelStack, histogram: &impl Histogram) {
        for bucket in histogram.buckets() {
            let upper = bucket.upper.to_string();
            let labels = labels.with("le", &upper);
            self.write_value(name, "_bucket", &labels, bucket.count);
        }
        self.write_value(name, "_sum", labels, histogram.sum());
        self.write_value(name, "_count", labels, histogram.count());
    }

    fn end_values(&mut self) {}

    fn into_output(self) -> String {
        self.output
    }
}

/// A [MetricsFormatter] for output in a bespoke JSON format.
pub struct JsonFormatter {
    output: String,
}

impl JsonFormatter {
    fn start_value(&mut self, labels: &LabelStack) {
        if !self.output.ends_with('[') {
            self.output.push(',');
        }
        self.output.push_str("{\"labels\":{");
        let mut index = 0;
        labels.iterate(&mut |name, value| {
            if index != 0 {
                self.output.write_char(',').unwrap();
            }
            index += 1;
            write!(
                &mut self.output,
                "{}:{}",
                JsonString(name),
                JsonString(value)
            )
            .unwrap();
        });
        write!(&mut self.output, "}},\"value\":").unwrap();
    }
}

impl MetricsFormatter for JsonFormatter {
    fn new() -> Self {
        Self {
            output: String::from("["),
        }
    }
    fn write_description(&mut self, name: &str, help: &str, metric_type: &str) {
        if self.output.len() > 1 {
            self.output.push(',');
        }
        write!(
            &mut self.output,
            "{{\"key\":{},\"type\":{}",
            JsonString(name),
            JsonString(metric_type)
        )
        .unwrap();
        if !help.is_empty() {
            write!(&mut self.output, ",\"description\":{}", JsonString(help)).unwrap();
        }
        write!(&mut self.output, ",\"values\":[").unwrap();
    }
    fn write_value(&mut self, _name: &str, _suffix: &str, labels: &LabelStack, value: f64) {
        self.start_value(labels);
        write!(&mut self.output, "{}}}", JsonNumber(value)).unwrap();
    }

    fn write_histogram(&mut self, _name: &str, labels: &LabelStack, histogram: &impl Histogram) {
        self.start_value(labels);
        self.output.push_str("{\"buckets\":[");
        let mut prev: Option<Bucket> = None;
        let mut n = 0;
        for bucket in histogram.buckets() {
            let count = match prev {
                Some(prev) => bucket.count - prev.count,
                None => bucket.count,
            };
            if count != 0.0 {
                if n != 0 {
                    self.output.push(',');
                }
                n += 1;

                self.output.push('{');
                if let Some(prev) = prev {
                    write!(&mut self.output, "\"gt\":{},", JsonNumber(prev.upper)).unwrap();
                }
                if bucket.upper.is_finite() {
                    write!(&mut self.output, "\"le\":{},", bucket.upper).unwrap();
                }
                write!(&mut self.output, "\"count\":{}}}", JsonNumber(count)).unwrap();
            }
            prev = Some(bucket);
        }
        write!(
            &mut self.output,
            "],\"sum\":{},\"count\":{}}}}}",
            JsonNumber(histogram.sum()),
            JsonNumber(histogram.count())
        )
        .unwrap();
    }

    fn end_values(&mut self) {
        self.output.push_str("]}");
    }

    fn into_output(mut self) -> String {
        self.output.push(']');
        self.output
    }
}

/// Displays an `f64` in JSON format.
///
/// JSON doesn't support infinities or NaN as numbers, so this writes them as
/// strings instead.
struct JsonNumber(f64);

impl Display for JsonNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_finite() {
            write!(f, "{}", self.0)
        } else if self.0.is_nan() {
            f.write_str("\"nan\"")
        } else if self.0.is_sign_negative() {
            f.write_str("\"-inf\"")
        } else {
            f.write_str("\"inf\"")
        }
    }
}

struct JsonString<'a>(&'a str);

impl<'a> Display for JsonString<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char('"')?;
        for c in self.0.chars() {
            match c {
                '\\' => write!(f, "\\\\"),
                '\n' => write!(f, "\\n"),
                _ => f.write_char(c),
            }?;
        }
        f.write_char('"')?;
        Ok(())
    }
}

/// A writer for metrics.
///
/// This composes the metrics in an internal buffer and yields them when
/// consumed with [MetricsWriter::into_output].
pub struct MetricsWriter<F> {
    formatter: F,
}

impl<F> Default for MetricsWriter<F>
where
    F: MetricsFormatter,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<F> MetricsWriter<F>
where
    F: MetricsFormatter,
{
    /// Creates a new metrics writer for format `F`.
    pub fn new() -> Self {
        Self {
            formatter: F::new(),
        }
    }

    /// Adds a collection of counters for the metric with the given `name`.
    /// Supply `help` as a human-readable text string explaining the counter.
    /// `write_values` should call [ValueWriter::write_value] for each value of
    /// the counter (each value should have different labels).
    ///
    /// The values have to be specified together in a callback because the
    /// [Prometheus exposition format] requires that all of the values for a
    /// given counter be written together in one block.
    ///
    /// [Prometheus exposition format]: https://prometheus.io/docs/instrumenting/exposition_formats/
    pub fn counter<W>(&mut self, name: &str, help: &str, write_values: W)
    where
        W: FnOnce(&mut ValueWriter<F>),
    {
        self.formatter.write_description(name, help, "counter");
        write_values(&mut ValueWriter {
            name,
            formatter: &mut self.formatter,
        });
        self.formatter.end_values();
    }

    /// Adds a collection of gauges for the metric with the given `name`.
    /// Supply `help` as a human-readable text string explaining the gauge.
    /// `write_values` should call [ValueWriter::write_value] for each value of
    /// the gauge (each value should have different labels).
    ///
    /// The values have to be specified together in a callback because the
    /// [Prometheus exposition format] requires that all of the values for a
    /// given gauge be written together in one block.
    ///
    /// [Prometheus exposition format]: https://prometheus.io/docs/instrumenting/exposition_formats/
    pub fn gauge<W>(&mut self, name: &str, help: &str, write_values: W)
    where
        W: FnOnce(&mut ValueWriter<F>),
    {
        self.formatter.write_description(name, help, "gauge");
        write_values(&mut ValueWriter {
            name,
            formatter: &mut self.formatter,
        });
        self.formatter.end_values();
    }

    /// Adds a collection of histograms for the metric with the given `name`.
    /// Supply `help` as a human-readable text string explaining the histogram.
    /// `write_values` should call [HistogramWriter::write_histogram] for each value
    /// of the histogram (each value should have different labels).
    ///
    /// The values have to be specified together in a callback because the
    /// [Prometheus exposition format] requires that all of the values for a
    /// given histogram be written together in one block.
    ///
    /// [Prometheus exposition format]: https://prometheus.io/docs/instrumenting/exposition_formats/
    pub fn histogram<W>(&mut self, name: &str, help: &str, f: W)
    where
        W: FnOnce(&mut HistogramWriter<F>),
    {
        self.formatter.write_description(name, help, "histogram");
        f(&mut HistogramWriter {
            formatter: &mut self.formatter,
            name,
        });
        self.formatter.end_values();
    }

    /// Consumes this metrics writer and returns the output.
    pub fn into_output(self) -> String {
        self.formatter.into_output()
    }
}

/// Transforms `.0` into the form required for a Prometheus identifier.
struct EscapedName<'a>(&'a str);

impl Display for EscapedName<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Convert nonalphanumerics to '_' and collapse adjacent '_'.
        let mut escaped = self
            .0
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .coalesce(|x, y| {
                if x == '_' && y == '_' {
                    Ok('_')
                } else {
                    Err((x, y))
                }
            })
            .peekable();
        match escaped.peek() {
            None => write!(f, "unnamed")?,
            Some('0'..='9') => write!(f, "_")?,
            Some(_) => (),
        };
        for c in escaped {
            f.write_char(c)?;
        }
        Ok(())
    }
}

/// Transforms `.0` as required for Prometheus help strings.
struct EscapedHelp<'a>(&'a str);

impl Display for EscapedHelp<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for c in self.0.chars() {
            match c {
                '\\' => write!(f, "\\\\"),
                '\n' => write!(f, "\\n"),
                _ => f.write_char(c),
            }?;
        }
        Ok(())
    }
}

/// A stack of labels.
#[derive(Clone, Debug)]
pub enum LabelStack<'a> {
    /// A nonempty stack.
    Label {
        /// Label name.
        name: &'a str,
        /// Label value.
        value: &'a str,
        /// The next label down the stack.
        next: &'a LabelStack<'a>,
    },
    /// An empty stack.
    End,
}

impl<'a> Default for LabelStack<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> LabelStack<'a> {
    /// Construct an empty stack of labels.
    pub fn new() -> Self {
        Self::End
    }

    /// Returns true if the stack is empty.
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::End)
    }

    /// Returns a label stack with `name` and `value` pushed on the top of `self`.
    pub fn with(&'a self, name: &'a str, value: &'a str) -> LabelStack<'a> {
        LabelStack::Label {
            name,
            value,
            next: self,
        }
    }
    fn iterate<F>(&self, f: &mut F)
    where
        F: FnMut(&str, &str),
    {
        if let LabelStack::Label { name, value, next } = self {
            next.iterate(f);
            f(name, value);
        }
    }
}

/// Transforms `.0` into the form required for a Prometheus label value.
struct EscapedValue<'a>(&'a str);

impl<'a> Display for EscapedValue<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for c in self.0.chars() {
            match c {
                '\\' => write!(f, "\\\\"),
                '\n' => write!(f, "\\n"),
                '"' => write!(f, "\\\""),
                _ => f.write_char(c),
            }?;
        }
        Ok(())
    }
}

/// Passed to [MetricsWriter::counter] and [MetricsWriter::gauge] callback.
pub struct ValueWriter<'a, F>
where
    F: MetricsFormatter,
{
    formatter: &'a mut F,
    name: &'a str,
}

impl<F> ValueWriter<'_, F>
where
    F: MetricsFormatter,
{
    /// Writes `value` as the value of this counter or gauge with the given
    /// `labels`.  Each call to `write_value` for a given counter or gauge
    /// should have different labels.
    pub fn write_value(&mut self, labels: &LabelStack, value: impl Value) {
        self.formatter
            .write_value(self.name, "", labels, value.as_f64());
    }
}

/// A type that can be written as the value of a metric.
///
/// All of these types are ultimately readable as a [f64].
pub trait Value {
    fn as_f64(&self) -> f64;
}

impl Value for f64 {
    fn as_f64(&self) -> f64 {
        *self
    }
}

macro_rules! from_cast {
    ($type_name:ty) => {
        impl Value for $type_name {
            fn as_f64(&self) -> f64 {
                *self as f64
            }
        }
    };
}
from_cast!(f32);
from_cast!(u8);
from_cast!(u16);
from_cast!(u32);
from_cast!(u64);
from_cast!(u128);
from_cast!(usize);
from_cast!(i8);
from_cast!(i16);
from_cast!(i32);
from_cast!(i64);
from_cast!(i128);
from_cast!(isize);

macro_rules! from_atomic {
    ($type_name:ty) => {
        impl Value for &$type_name {
            fn as_f64(&self) -> f64 {
                self.load(Ordering::Relaxed) as f64
            }
        }
    };
}

from_atomic!(AtomicU8);
from_atomic!(AtomicU16);
from_atomic!(AtomicU32);
from_atomic!(AtomicU64);
from_atomic!(AtomicI8);
from_atomic!(AtomicI16);
from_atomic!(AtomicI32);
from_atomic!(AtomicI64);

/// Passed to [MetricsWriter::histogram] callback.
pub struct HistogramWriter<'a, F> {
    formatter: &'a mut F,
    name: &'a str,
}

impl<'a, F> HistogramWriter<'a, F>
where
    F: MetricsFormatter,
{
    /// Writes `histogram` as the value of this counter or gauge with the given
    /// `labels`.  Each call to `write_histogram` for a given histogram should
    /// have different labels.
    pub fn write_histogram(&mut self, labels: &LabelStack, histogram: &impl Histogram) {
        self.formatter.write_histogram(self.name, labels, histogram);
    }
}

/// A bucket in a histogram.
#[derive(Copy, Clone, Debug)]
pub struct Bucket {
    /// The upper limit of the bucket.
    upper: f64,

    /// The cumulative count up to `upper`.
    count: f64,
}

/// A histogram for passing to [HistogramWriter].
pub trait Histogram {
    /// Sum of all the buckets.
    fn sum(&self) -> f64;
    /// Total count in the histogram.
    fn count(&self) -> f64;
    /// All the buckets in the histogram.  The final bucket should have upper
    /// limit [f64::INFINITY] and count equal to [count][Self::count].
    fn buckets(&self) -> impl Iterator<Item = Bucket>;
}

#[cfg(test)]
mod tests {
    use crate::server::metrics::{
        Bucket, Histogram, JsonFormatter, LabelStack, MetricsFormatter, MetricsWriter,
        PrometheusFormatter,
    };

    #[test]
    fn prometheus_writer() {
        assert_eq!(
            write_metrics::<PrometheusFormatter>(),
            r#"# HELP http_requests_total The total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total{method="post",code="200"} 1027
http_requests_total{method="post",code="400"} 3

# HELP http_request_duration_seconds A histogram of the request duration.
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.05"} 24054
http_request_duration_seconds_bucket{le="0.1"} 33444
http_request_duration_seconds_bucket{le="0.2"} 100392
http_request_duration_seconds_bucket{le="0.5"} 129389
http_request_duration_seconds_bucket{le="1"} 133988
http_request_duration_seconds_bucket{le="inf"} 144320
http_request_duration_seconds_sum 53423
http_request_duration_seconds_count 144320
"#
        );
    }

    #[test]
    fn json_writer() {
        assert_eq!(
            write_metrics::<JsonFormatter>(),
            r#"[{"key":"http_requests_total","type":"counter","description":"The total number of HTTP requests","values":[{"labels":{"method":"post","code":"200"},"value":1027},{"labels":{"method":"post","code":"400"},"value":3}]},{"key":"http_request_duration_seconds","type":"histogram","description":"A histogram of the request duration.","values":[{"labels":{},"value":{"buckets":[{"le":0.05,"count":24054},{"gt":0.05,"le":0.1,"count":9390},{"gt":0.1,"le":0.2,"count":66948},{"gt":0.2,"le":0.5,"count":28997},{"gt":0.5,"le":1,"count":4599},{"gt":1,"count":10332}],"sum":53423,"count":144320}}]}]"#
        );
    }

    fn write_metrics<F>() -> String
    where
        F: MetricsFormatter,
    {
        let mut metrics_writer = MetricsWriter::<F>::new();
        let binding = LabelStack::new();
        let labels = binding.with("method", "post");
        metrics_writer.counter(
            "http_requests_total",
            "The total number of HTTP requests",
            |counter| {
                counter.write_value(&labels.with("code", "200"), 1027);
                counter.write_value(&labels.with("code", "400"), 3)
            },
        );

        struct H;
        impl Histogram for H {
            fn sum(&self) -> f64 {
                53423.0
            }

            fn count(&self) -> f64 {
                144320.0
            }

            fn buckets(&self) -> impl Iterator<Item = Bucket> {
                [
                    Bucket {
                        upper: 0.05,
                        count: 24054.0,
                    },
                    Bucket {
                        upper: 0.1,
                        count: 33444.0,
                    },
                    Bucket {
                        upper: 0.2,
                        count: 100392.0,
                    },
                    Bucket {
                        upper: 0.5,
                        count: 129389.0,
                    },
                    Bucket {
                        upper: 1.0,
                        count: 133988.0,
                    },
                    Bucket {
                        upper: f64::INFINITY,
                        count: 144320.0,
                    },
                ]
                .into_iter()
            }
        }

        metrics_writer.histogram(
            "http_request_duration_seconds",
            "A histogram of the request duration.",
            |h| h.write_histogram(&LabelStack::new(), &H),
        );

        metrics_writer.into_output()
    }
}
