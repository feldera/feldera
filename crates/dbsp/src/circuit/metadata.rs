use size_of::{HumanBytes, TotalSize};
use std::{
    borrow::Cow,
    fmt::{self, Write},
    ops::{Deref, DerefMut},
    panic::Location,
    time::Duration,
};

/// Attribute that represents the total number of bytes used by a stateful
/// operator. This includes bytes used to store the actual state, but not the
/// excess pre-allocated capacity available inside operator's internal data
/// structures (see [`ALLOCATED_BYTES_LABEL`]).
pub const USED_BYTES_LABEL: &str = "used bytes";

/// Attribute that represents the total number of bytes allocated by a stateful
/// operator. This value can be larger than the number of bytes used by the
/// operator since it includes excess pre-allocated capacity inside operator's
/// internal data structures (see [`USED_BYTES_LABEL`]).
pub const ALLOCATED_BYTES_LABEL: &str = "allocated bytes";

/// Attribute that represents the number of shared bytes used by a stateful
/// operator, i.e., bytes that are shared behind things like `Arc` or `Rc`.
pub const SHARED_BYTES_LABEL: &str = "shared bytes";

/// Attribute that represents the number of entries stored by a stateful
/// operator, e.g., the number of entries in a trace.
pub const NUM_ENTRIES_LABEL: &str = "total size";

/// An operator's location within the source program
pub type OperatorLocation = Option<&'static Location<'static>>;

/// The label to a metadata item
pub type MetaLabel = Cow<'static, str>;

/// General metadata about an operator's execution
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OperatorMeta {
    entries: Vec<(MetaLabel, MetaItem)>,
}

impl OperatorMeta {
    /// Create a new `OperatorMeta`
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Create a new `OperatorMeta` with space for `capacity` entries
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    pub fn get(&self, attribute: &str) -> Option<MetaItem> {
        self.entries
            .iter()
            .find(|(label, _item)| label == attribute)
            .map(|(_label, item)| item.clone())
    }

    pub fn merge(&mut self, other: &Self) {
        for (label, src) in &other.entries {
            if src.is_mergeable() {
                if let Some(dst_index) = self
                    .entries
                    .iter_mut()
                    .position(|(label2, _item)| label == label2)
                {
                    let (_label, ref mut dst) = &mut self.entries[dst_index];
                    if let Some(merged) = src.merge(dst) {
                        *dst = merged;
                    } else {
                        self.entries.remove(dst_index);
                    }
                } else {
                    self.entries.push((label.clone(), src.clone()));
                }
            }
        }
    }
}

impl Deref for OperatorMeta {
    type Target = Vec<(MetaLabel, MetaItem)>;

    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl DerefMut for OperatorMeta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entries
    }
}

impl Extend<(MetaLabel, MetaItem)> for OperatorMeta {
    fn extend<T>(&mut self, iter: T)
    where
        T: IntoIterator<Item = (MetaLabel, MetaItem)>,
    {
        self.entries.extend(iter);
    }
}

impl<const N: usize> From<[(MetaLabel, MetaItem); N]> for OperatorMeta {
    fn from(array: [(MetaLabel, MetaItem); N]) -> Self {
        let mut this = Self::with_capacity(N);
        this.extend(array);
        this
    }
}

impl<'a> From<&'a [(MetaLabel, MetaItem)]> for OperatorMeta {
    fn from(slice: &'a [(MetaLabel, MetaItem)]) -> Self {
        let mut this = Self::with_capacity(slice.len());
        this.entries.clone_from_slice(slice);
        this
    }
}

impl From<Vec<(MetaLabel, MetaItem)>> for OperatorMeta {
    fn from(entries: Vec<(MetaLabel, MetaItem)>) -> Self {
        Self { entries }
    }
}

/// An operator metadata entry
#[derive(Debug, Clone, PartialEq)]
pub enum MetaItem {
    /// An integer with no particular semantics.
    Int(usize),

    /// An integer count of something.
    ///
    /// This should be used for kinds of things that make sense to summarize by
    /// adding, e.g. counts of allocations or stored batches.
    Count(usize),

    /// A percentage in terms of a numerator and denominator. Separating these
    /// makes it possible to aggregate them.
    Percent {
        numerator: u64,
        denominator: u64,
    },

    String(String),
    Array(Vec<Self>),
    Map(OperatorMeta),
    Bytes(HumanBytes),
    Duration(Duration),
}

impl MetaItem {
    pub fn bytes(bytes: usize) -> Self {
        Self::Bytes(HumanBytes::from(bytes))
    }

    pub fn format(&self, output: &mut dyn Write) -> fmt::Result {
        match self {
            Self::Int(int) | Self::Count(int) => write!(output, "{int}"),
            Self::Percent {
                numerator,
                denominator,
            } => {
                let percent = (*numerator as f64) / (*denominator as f64) * 100.0;
                if !percent.is_nan() && !percent.is_infinite() {
                    write!(output, "{percent:.02}%")
                } else {
                    write!(output, "(undefined)")
                }
            }
            Self::String(string) => output.write_str(string),
            Self::Bytes(bytes) => write!(output, "{bytes}"),
            Self::Duration(duration) => write!(output, "{duration:#?}"),

            Self::Array(array) => {
                output.write_char('[')?;
                for (idx, item) in array.iter().enumerate() {
                    item.format(output)?;
                    if idx != array.len() - 1 {
                        output.write_str(", ")?
                    }
                }
                output.write_char(']')
            }

            Self::Map(map) => {
                output.write_char('{')?;
                for (idx, (label, item)) in map.iter().enumerate() {
                    output.write_str(label)?;
                    output.write_str(": ")?;
                    item.format(output)?;

                    if idx != map.len() - 1 {
                        output.write_str(", ")?;
                    }
                }
                output.write_char('}')
            }
        }
    }

    pub fn is_mergeable(&self) -> bool {
        matches!(
            self,
            MetaItem::Count(_)
                | MetaItem::Bytes(_)
                | MetaItem::Duration(_)
                | MetaItem::Percent { .. }
        )
    }

    pub fn merge(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (Self::Count(a), Self::Count(b)) => Some(Self::Count(a + b)),
            (
                Self::Percent {
                    numerator: an,
                    denominator: ad,
                },
                Self::Percent {
                    numerator: bn,
                    denominator: bd,
                },
            ) => Some(Self::Percent {
                numerator: an + bn,
                denominator: ad + bd,
            }),
            (Self::Bytes(a), Self::Bytes(b)) => Some(Self::Bytes(HumanBytes {
                bytes: a.bytes + b.bytes,
            })),
            (Self::Duration(a), Self::Duration(b)) => Some(Self::Duration(a.saturating_add(*b))),
            _ => None,
        }
    }
}

impl Default for MetaItem {
    fn default() -> Self {
        Self::String(String::new())
    }
}

impl From<Duration> for MetaItem {
    fn from(duration: Duration) -> Self {
        Self::Duration(duration)
    }
}

impl From<HumanBytes> for MetaItem {
    fn from(bytes: HumanBytes) -> Self {
        Self::Bytes(bytes)
    }
}

impl From<String> for MetaItem {
    fn from(string: String) -> Self {
        Self::String(string)
    }
}

impl From<usize> for MetaItem {
    fn from(int: usize) -> Self {
        Self::Int(int)
    }
}

#[macro_export]
macro_rules! metadata {
    ($($name:expr => $value:expr),* $(,)?) => {
        [$((::std::borrow::Cow::from($name), $crate::circuit::metadata::MetaItem::from($value)),)*]
    };
}

impl From<TotalSize> for MetaItem {
    fn from(size: TotalSize) -> Self {
        Self::Map(
            metadata! {
                "allocated bytes" => Self::bytes(size.total_bytes()),
                "used bytes" => Self::bytes(size.used_bytes()),
                "allocations" => Self::Count(size.distinct_allocations()),
                "shared bytes" => Self::bytes(size.shared_bytes()),
            }
            .into(),
        )
    }
}
