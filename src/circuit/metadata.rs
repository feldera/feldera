use size_of::{HumanBytes, TotalSize};
use std::{
    borrow::Cow,
    fmt::{self, Write},
    panic::Location,
    time::Duration,
};

/// An operator's location within the source program
pub type OperatorLocation = Option<&'static Location<'static>>;

/// General metadata about an operator's execution
pub type OperatorMeta = Vec<(Cow<'static, str>, MetaItem)>;

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
            .to_vec(),
        )
    }
}
