use std::{ops::Deref, fmt::{Display, Formatter, Result as FmtResult, Debug}};

use arcstr::ArcStr as Inner;
use bincode::{Encode, Decode, enc::Encoder, error::{EncodeError, DecodeError}, de::{Decoder, BorrowDecoder}, BorrowDecode};
use size_of::SizeOf;

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default, SizeOf)]
pub struct ArcStr(pub Inner);

impl ArcStr {
    pub fn new() -> Self { Self(Inner::new()) }
}

impl Deref for ArcStr {
    type Target = Inner;
    fn deref(&self) -> &Self::Target { &self.0 }
}

impl Display for ArcStr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{}", &self.0)
    }
}

impl Debug for ArcStr {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{:?}", &self.0)
    }
}

impl From<String> for ArcStr {
    fn from(source: String) -> Self {
        ArcStr(Inner::from(source))
    }
}

impl From<&str> for ArcStr {
    fn from(source: &str) -> Self {
        ArcStr(Inner::from(source))
    }
}

impl bincode::Encode for ArcStr {
    fn encode<E: Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), EncodeError> {
        Encode::encode(self.0.as_str(), encoder)?;
        Ok(())
    }
}

impl PartialEq<&str> for ArcStr {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl Decode for ArcStr {
    fn decode<D: Decoder>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let str: String = Decode::decode(decoder)?;
        Ok(str.into())
    }
}

impl<'de> BorrowDecode<'de> for ArcStr {
    fn borrow_decode<D: BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        let str: String = Decode::decode(decoder)?;
        Ok(str.into())
    }
}

// Import and rename the macros from `arcstr` for wrapping.
//
// If we instead wrap the macros directly, e.g. if we use `::arcstr::literal` in
// the definition below, then code that depends on us needs to add a direct
// dependency on `arcstr`, but `cargo machete` will report that the dependency
// is unused because the only reference appears in a macro expansion.
//
// The imports need to be `pub` because names in macro expansions aren't
// privileged.
pub use arcstr::literal as arcstr_inner_literal;
pub use arcstr::format as arcstr_inner_format;

#[macro_export]
macro_rules! arcstr_literal {
    ($text:expr $(,)?) => {{
        $crate::algebra::ArcStr($crate::algebra::arcstr::arcstr_inner_literal!($text))
    }};
}

#[macro_export]
macro_rules! arcstr_format {
    ($($toks:tt)*) => {
        $crate::algebra::ArcStr($crate::algebra::arcstr::arcstr_inner_format!($($toks)*))
    };
}
