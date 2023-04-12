use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

bitflags::bitflags! {
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Clone, Copy, Deserialize, Serialize)]
    #[serde(try_from = "String", into = "String")]
    pub struct InputFlags: u8 {
        /// The parameter can be used as an input
        const INPUT = 1 << 0;
        /// The parameter can be used as an output
        const OUTPUT = 1 << 1;
        /// The parameter can be used as both an input and output
        const INOUT = Self::INPUT.bits() | Self::OUTPUT.bits();
    }
}

impl InputFlags {
    pub const fn is_input(&self) -> bool {
        self.contains(Self::INPUT)
    }

    pub const fn is_output(&self) -> bool {
        self.contains(Self::OUTPUT)
    }

    pub const fn is_inout(&self) -> bool {
        self.contains(Self::INOUT)
    }

    /// Returns `true` if the parameter is only a input and not an output
    pub const fn is_readonly(&self) -> bool {
        self.is_input() && !self.is_output()
    }
}

#[derive(Debug, Clone)]
pub struct InvalidInputFlag(Box<str>);

impl Display for InvalidInputFlag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Invalid input flag, expected one of \"input\", \"output\" or \"inout\", got {:?}",
            self.0,
        )
    }
}

// TODO: Maybe this would be better represented as a comma-delimited list, e.g.
// `"input,output"`
impl TryFrom<&str> for InputFlags {
    type Error = InvalidInputFlag;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(match value {
            "" => Self::empty(),
            "input" => Self::INPUT,
            "output" => Self::OUTPUT,
            "inout" => Self::INOUT,
            invalid => return Err(InvalidInputFlag(Box::from(invalid))),
        })
    }
}

impl TryFrom<String> for InputFlags {
    type Error = InvalidInputFlag;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(&*value)
    }
}

impl From<InputFlags> for &'static str {
    fn from(flags: InputFlags) -> Self {
        match flags {
            InputFlags::INPUT => "input",
            InputFlags::OUTPUT => "output",
            InputFlags::INOUT => "inout",
            _ => unreachable!(),
        }
    }
}

impl From<InputFlags> for String {
    fn from(flags: InputFlags) -> Self {
        <&'static str>::from(flags).to_owned()
    }
}
