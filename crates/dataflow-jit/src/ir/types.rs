use crate::{
    codegen::NativeType,
    ir::{
        function::InputFlags,
        pretty::{DocAllocator, DocBuilder, Pretty},
        LayoutId, RowLayoutCache,
    },
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display, Write};

macro_rules! column_type {
    ($($(#[$meta:meta])* $column_ty:ident = ($display:literal, $native_ty:expr)),+ $(,)?) => {
        /// The type of a single column within a row
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, JsonSchema)]
        pub enum ColumnType {
            $(
                $(#[$meta])*
                $column_ty,
            )+
        }

        impl ColumnType {
            /// Returns the pretty name of the column type
            #[must_use]
            pub const fn to_str(self) -> &'static str {
                match self {
                    $(Self::$column_ty => $display,)+
                }
            }

            /// Returns the [`NativeType`] that corresponds with the current `ColumnType`,
            /// returning `None` if there's no equivalent `NativeType`.
            ///
            /// Currently [`Unit`][ColumnType::Unit] is the only type that will return
            /// `None`, as zero sized types have no runtime representation
            #[must_use]
            pub const fn native_type(self) -> Option<NativeType> {
                use NativeType::*;
                Some(match self {
                    $(Self::$column_ty => $native_ty,)+
                })
            }

            paste::paste! {
                $(
                    #[doc = "Returns `true` if the current column type is a [`" $column_ty "`][ColumnType::" $column_ty "]"]
                    #[must_use]
                    pub const fn [<is_ $column_ty:lower>](&self) -> bool {
                        matches!(self, Self::$column_ty)
                    }
                )+
            }

        }
    };
}

column_type! {
    /// A boolean value (either zero for `false` or one for `true`)
    Bool = ("bool", Bool),

    /// An unsigned 8 bit integer
    U8 = ("u8", U8),
    /// A signed 8 bit integer
    I8 = ("i8", I8),
    /// An unsigned 16 bit integer
    U16 = ("u16", U16),
    /// A signed 16 bit integer
    I16 = ("i16", I16),
    /// An unsigned 32 bit integer
    U32 = ("u32", U32),
    /// A signed 32 bit integer
    I32 = ("i32", I32),
    /// An unsigned 64 bit integer
    U64 = ("u64", U64),
    /// A signed 64 bit integer
    I64 = ("i64", I64),
    /// An unsigned pointer-width integer
    Usize = ("usize", Usize),
    /// A signed pointer-width integer
    Isize = ("isize", Isize),

    /// A 32 bit floating point value
    F32 = ("f32", F32),
    /// A 64 bit floating point value
    F64 = ("f64", F64),

    /// Represents the days since Jan 1 1970 as an `i32`
    Date = ("date", I32),
    /// Represents the milliseconds since Jan 1 1970 as an `i64`
    Timestamp = ("timestamp", I64),

    /// A string encoded as UTF-8
    String = ("str", Ptr),

    /// A unit value
    Unit = ("unit", return None),

    /// A raw pointer value
    Ptr = ("ptr", Ptr),
}

impl ColumnType {
    /// Returns `true` if the column type is an integer of any width (signed or
    /// unsigned)
    #[must_use]
    pub const fn is_int(self) -> bool {
        matches!(
            self,
            Self::U8
                | Self::I8
                | Self::U16
                | Self::I16
                | Self::U32
                | Self::I32
                | Self::U64
                | Self::I64
                | Self::Usize
                | Self::Isize,
        )
    }

    /// Returns `true` if the column type is a signed integer of any width
    #[must_use]
    pub const fn is_signed_int(self) -> bool {
        matches!(
            self,
            Self::I8 | Self::I16 | Self::I32 | Self::I64 | Self::Isize,
        )
    }

    /// Returns `true` if the column type is an unsigned integer of any width
    #[must_use]
    pub const fn is_unsigned_int(self) -> bool {
        matches!(
            self,
            Self::U8 | Self::U16 | Self::U32 | Self::U64 | Self::Usize,
        )
    }

    /// Returns `true` if the column type is a floating point value
    #[must_use]
    pub const fn is_float(self) -> bool {
        matches!(self, Self::F32 | Self::F64)
    }

    /// Returns `true` if the column type requires a non-trivial drop
    /// operation (currently just [`String`][ColumnType::String])
    #[must_use]
    pub const fn needs_drop(&self) -> bool {
        matches!(self, Self::String)
    }

    /// Returns `true` if the column type requires a non-trivial clone
    /// operation (currently just [`String`][ColumnType::String])
    #[must_use]
    pub const fn requires_nontrivial_clone(&self) -> bool {
        matches!(self, Self::String)
    }

    /// Returns `true` if the column type is a zero-sized type
    /// (currently just [`Unit`][ColumnType::Unit])
    #[must_use]
    pub const fn is_zst(&self) -> bool {
        matches!(self, Self::Unit)
    }
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_str())
    }
}

impl<'a, D, A> Pretty<'a, D, A> for ColumnType
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, _cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc.text(self.to_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    args: Vec<LayoutId>,
    arg_flags: Vec<InputFlags>,
    ret: ColumnType,
}

impl Signature {
    pub fn new(args: Vec<LayoutId>, arg_flags: Vec<InputFlags>, ret: ColumnType) -> Self {
        Self {
            args,
            arg_flags,
            ret,
        }
    }

    pub fn args(&self) -> &[LayoutId] {
        &self.args
    }

    pub fn arg_flags(&self) -> &[InputFlags] {
        &self.arg_flags
    }

    pub fn ret(&self) -> ColumnType {
        self.ret
    }

    pub(crate) fn display<'a>(&'a self, layout_cache: &'a RowLayoutCache) -> impl Display + 'a {
        struct DisplaySig<'a>(&'a Signature, &'a RowLayoutCache);

        impl Display for DisplaySig<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("fn(")?;
                for (idx, (&layout_id, &flags)) in
                    self.0.args.iter().zip(&self.0.arg_flags).enumerate()
                {
                    let mut has_prefix = false;
                    if flags.contains(InputFlags::INPUT) {
                        f.write_str("in")?;
                        has_prefix = true;
                    }
                    if flags.contains(InputFlags::OUTPUT) {
                        f.write_str("out")?;
                        has_prefix = true;
                    }
                    if has_prefix {
                        f.write_char(' ')?;
                    }

                    let layout = self.1.get(layout_id);
                    write!(f, "{layout:?}")?;

                    if idx != self.0.args.len() - 1 {
                        f.write_str(", ")?;
                    }
                }
                f.write_char(')')?;

                if self.0.ret != ColumnType::Unit {
                    f.write_str(" -> ")?;
                    write!(f, "{}", self.0.ret)?;
                }

                Ok(())
            }
        }

        DisplaySig(self, layout_cache)
    }
}
