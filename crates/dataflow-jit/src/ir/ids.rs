//! Defines id types used within the jit

/// Creates an id type and a corresponding id generator
macro_rules! create_ids {
    ($($name:ident = $prefix:literal),* $(,)?) => {

        ::paste::paste! {
            $(
                #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
                #[repr(transparent)]
                pub struct $name(::std::num::NonZeroU32);

                impl $name {
                    #[doc = "Creates a `" $name "` with a value of `u32::MAX`, used for placeholders but can also potentially be a valid id"]
                    #[allow(dead_code)]
                    pub(crate) const MAX: Self = {
                        // Safety: u32::MAX != 0
                        Self(unsafe { ::std::num::NonZeroU32::new_unchecked(u32::MAX) })
                    };
                }

                impl ::std::fmt::Debug for $name {
                    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                        ::std::fmt::Display::fmt(self, f)
                    }
                }

                impl ::std::fmt::Display for $name {
                    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                        write!(f, concat!($prefix, "{}"), self.0.get())
                    }
                }

                #[doc = "A generator for [`" $name "`]s"]
                pub struct [<$name Gen>] {
                    id: u32,
                }

                impl [<$name Gen>] {
                    #[doc = "Creates a new `" [<$name Gen>] "` to generate [`" $name "`]s"]
                    pub const fn new() -> Self {
                        Self { id: 1 }
                    }

                    #[doc = "Generates the next [`" $name "`]\n\n## Panics\n\nPanics if more than `2³²-1` ids are created"]
                    pub fn next(&mut self) -> $name {
                        let id = self.id;
                        self.id = match id.checked_add(1) {
                            Some(id) => id,
                            None => id_generator_overflow(stringify!($name)),
                        };
                        debug_assert_ne!(id, 0);

                        // Safety: `id` starts at 1 and will never overflow
                        $name(unsafe { ::std::num::NonZeroU32::new_unchecked(id) })
                    }
                }

                impl ::std::fmt::Debug for [<$name Gen>] {
                    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
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
    BlockId  = "bb",
    LayoutId = "layout",
}
