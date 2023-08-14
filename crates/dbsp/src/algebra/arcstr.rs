use std::{
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    ops::Deref,
};

use arcstr::ArcStr as Inner;
use rkyv::{
    string::{ArchivedString, StringResolver},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
    Archive, Deserialize, Fallible, Serialize, SerializeUnsized,
};
use size_of::SizeOf;

#[derive(
    Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default, SizeOf, Archive, Serialize, Deserialize,
)]
pub struct ArcStr(#[with(AsString)] pub Inner);

impl ArcStr {
    pub fn new() -> Self {
        Self(Inner::new())
    }
}

impl Deref for ArcStr {
    type Target = Inner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
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

impl PartialEq<&str> for ArcStr {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
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
pub use arcstr::format as arcstr_inner_format;
pub use arcstr::literal as arcstr_inner_literal;

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
/*
struct RkyvAsString;

impl ArchiveWith<Inner> for RkyvAsString {
    type Archived = Archived<String>;
    type Resolver = Resolver<String>;

    unsafe fn resolve_with(field: &Inner, pos: usize, resolver: StringResolver, out: *mut Self::Archived) {
        let s = String::from(field.as_str());
        s.resolve(pos, resolver, out);
    }
}

impl<S: Fallible + ?Sized> SerializeWith<Inner, S> for RkyvAsString
{
    fn serialize_with(field: &Inner, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        let s = ArchivedString::from(field.as_str());
        s.serialize(serializer)
    }
}

impl<D: Fallible + ?Sized> DeserializeWith<Archived<String>, Inner, D> for RkyvAsString
{
    fn deserialize_with(field: &Archived<String>, deserializer: &mut D) -> Result<Inner, D::Error> {
        let s = field.deserialize(deserializer)?;
        Ok(Inner::new(s))
    }
}
*/

#[derive(Debug)]
pub struct AsString;

impl ArchiveWith<Inner> for AsString {
    type Archived = ArchivedString;
    type Resolver = StringResolver;

    #[inline]
    unsafe fn resolve_with(
        field: &Inner,
        pos: usize,
        resolver: Self::Resolver,
        out: *mut Self::Archived,
    ) {
        ArchivedString::resolve_from_str(field.as_str(), pos, resolver, out);
    }
}

impl<S: Fallible + ?Sized> SerializeWith<Inner, S> for AsString
where
    str: SerializeUnsized<S>,
{
    #[inline]
    fn serialize_with(field: &Inner, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        ArchivedString::serialize_from_str(field.as_str(), serializer)
    }
}

impl<D: Fallible + ?Sized> DeserializeWith<ArchivedString, Inner, D> for AsString {
    #[inline]
    fn deserialize_with(field: &ArchivedString, _: &mut D) -> Result<Inner, D::Error> {
        Ok(Inner::from(field.as_str()))
    }
}
