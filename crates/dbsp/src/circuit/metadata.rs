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
    Int(usize),
    Percent(f64),
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
            Self::Int(int) => write!(output, "{int}"),
            Self::Percent(percent) => write!(output, "{percent:.02}%"),
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
                "allocations" => size.distinct_allocations(),
                "shared bytes" => Self::bytes(size.shared_bytes()),
            }
            .into(),
        )
    }
}
