use crate::ir::{
    layout_cache::RowLayoutCache,
    pretty::{DocAllocator, DocBuilder, Pretty},
    LayoutId,
};
use derive_more::{IsVariant, Unwrap};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Deserialize,
    Serialize,
    JsonSchema,
    IsVariant,
    Unwrap,
)]
pub enum StreamLayout {
    Set(LayoutId),
    Map(LayoutId, LayoutId),
}

impl StreamLayout {
    #[inline]
    pub const fn key_layout(self) -> LayoutId {
        match self {
            Self::Set(key) | Self::Map(key, _) => key,
        }
    }

    #[inline]
    pub const fn value_layout(self) -> Option<LayoutId> {
        match self {
            Self::Set(_) => None,
            Self::Map(_, value) => Some(value),
        }
    }

    #[inline]
    pub const fn kind(self) -> StreamKind {
        match self {
            Self::Set(_) => StreamKind::Set,
            Self::Map(_, _) => StreamKind::Map,
        }
    }

    #[inline]
    pub(crate) fn map_layouts<F>(self, map: &mut F)
    where
        F: FnMut(LayoutId) + ?Sized,
    {
        match self {
            Self::Set(key) => map(key),
            Self::Map(key, value) => {
                map(key);
                map(value);
            }
        }
    }

    #[inline]
    pub(crate) fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        match self {
            Self::Set(key) => *key = mappings[key],
            Self::Map(key, value) => {
                *key = mappings[key];
                *value = mappings[value];
            }
        }
    }

    #[track_caller]
    #[inline]
    pub fn expect_set(self, message: &str) -> LayoutId {
        #[inline(never)]
        #[track_caller]
        #[cold]
        fn panic_expect_set(key: LayoutId, value: LayoutId, message: &str) -> ! {
            panic!("called .expect_set() on a `StreamLayout::Map({key}, {value})`: {message}")
        }

        match self {
            Self::Set(key) => key,
            Self::Map(key, value) => panic_expect_set(key, value, message),
        }
    }

    #[track_caller]
    #[inline]
    pub fn expect_map(self, message: &str) -> (LayoutId, LayoutId) {
        #[inline(never)]
        #[track_caller]
        #[cold]
        fn panic_expect_set(key: LayoutId, message: &str) -> ! {
            panic!("called .expect_map() on a `StreamLayout::Set({key})`: {message}")
        }

        match self {
            Self::Map(key, value) => (key, value),
            Self::Set(key) => panic_expect_set(key, message),
        }
    }

    #[must_use]
    #[inline]
    pub const fn as_set(&self) -> Option<LayoutId> {
        if let Self::Set(layout) = *self {
            Some(layout)
        } else {
            None
        }
    }
}

impl<'a, D, A> Pretty<'a, D, A> for StreamLayout
where
    A: 'a,
    D: DocAllocator<'a, A> + ?Sized,
{
    fn pretty(self, alloc: &'a D, cache: &RowLayoutCache) -> DocBuilder<'a, D, A> {
        alloc.text(match self {
            Self::Set(key) => format!("{{ {} }}]", cache.get(key)),
            Self::Map(key, value) => {
                format!("{{ {}: {} }}", cache.get(key), cache.get(value))
            }
        })
    }
}

impl From<LayoutId> for StreamLayout {
    #[inline]
    fn from(key: LayoutId) -> Self {
        Self::Set(key)
    }
}

impl From<(LayoutId, LayoutId)> for StreamLayout {
    #[inline]
    fn from((key, value): (LayoutId, LayoutId)) -> Self {
        Self::Map(key, value)
    }
}

impl From<[LayoutId; 2]> for StreamLayout {
    #[inline]
    fn from([key, value]: [LayoutId; 2]) -> Self {
        Self::Map(key, value)
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Deserialize,
    Serialize,
    JsonSchema,
    IsVariant,
)]
pub enum StreamKind {
    Set,
    Map,
}
