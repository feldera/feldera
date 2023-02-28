//! Defines id types used within the jit

use serde::{Deserialize, Serialize};
use std::{
    cell::Cell,
    fmt::{self, Debug, Display},
    num::NonZeroU32,
    str::FromStr,
};

/// Creates an id type and a corresponding id generator
macro_rules! create_ids {
    ($($name:ident = $prefix:literal),* $(,)?) => {
        ::paste::paste! {
            $(
                #[derive(
                    Clone,
                    Copy,
                    PartialEq,
                    Eq,
                    PartialOrd,
                    Ord,
                    Hash,
                    Deserialize,
                    Serialize,
                )]
                #[serde(transparent)]
                #[repr(transparent)]
                pub struct $name(NonZeroU32);

                #[automatically_derived]
                #[allow(dead_code)]
                impl $name {
                    #[doc = "Creates a `" $name "` with a value of `u32::MAX`, used for placeholders but can also potentially be a valid id"]
                    pub(crate) const MAX: Self = {
                        // Safety: u32::MAX != 0
                        Self(unsafe { NonZeroU32::new_unchecked(u32::MAX) })
                    };

                    #[inline]
                    pub(crate) fn new(id: u32) -> Self {
                        Self(NonZeroU32::new(id).expect(concat!(
                            "created a ",
                            stringify!($name),
                            " from an id of zero",
                        )))
                    }

                    #[inline]
                    pub(crate) const fn into_inner(self) -> u32 {
                        self.0.get()
                    }
                }

                impl FromStr for $name {
                    type Err = <NonZeroU32 as FromStr>::Err;

                    #[inline]
                    fn from_str(string: &str) -> Result<Self, Self::Err> {
                        Ok(Self(string.trim_start_matches($prefix).parse()?))
                    }
                }

                impl Debug for $name {
                    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        Display::fmt(self, f)
                    }
                }

                impl Display for $name {
                    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        write!(f, concat!($prefix, "{}"), self.0.get())
                    }
                }

                #[doc = "A generator for [`" $name "`]s"]
                #[allow(dead_code)]
                pub struct [<$name Gen>] {
                    id: Cell<u32>,
                }

                #[automatically_derived]
                #[allow(dead_code)]
                impl [<$name Gen>] {
                    #[doc = "Creates a new `" [<$name Gen>] "` to generate [`" $name "`]s"]
                    #[inline]
                    pub const fn new() -> Self {
                        Self {
                            id: Cell::new(1),
                        }
                    }

                    #[inline]
                    pub(crate) fn after_id(id: NodeId) -> Self {
                        Self {
                            id: Cell::new(match id.into_inner().checked_add(1) {
                                Some(id) => id,
                                None => id_generator_overflow(stringify!($name)),
                            }),
                        }
                    }

                    #[doc = "Generates the next [`" $name "`]\n\n## Panics\n\nPanics if more than `2³²-1` ids are created"]
                    #[inline]
                    pub fn next(&self) -> $name {
                        let id = self.id.get();
                        self.id.set(match id.checked_add(1) {
                            Some(id) => id,
                            None => id_generator_overflow(stringify!($name)),
                        });
                        debug_assert_ne!(id, 0);

                        // Safety: `id` starts at 1 and will never overflow
                        $name(unsafe { NonZeroU32::new_unchecked(id) })
                    }
                }

                impl Debug for [<$name Gen>] {
                    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.debug_struct(stringify!([<$name Gen>]))
                            .finish_non_exhaustive()
                    }
                }
            )*
        }

        #[cold]
        #[inline(never)]
        fn id_generator_overflow(id: &'static str) -> ! {
            panic!("created more than {} {id}s", u32::MAX - 1)
        }
    };
}

create_ids! {
    NodeId   = "n",
    ExprId   = "v",
    // FuncId   = "fn",
    BlockId  = "bb",
    LayoutId = "layout",
}
