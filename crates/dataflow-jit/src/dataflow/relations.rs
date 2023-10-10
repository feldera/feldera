use crate::{
    ir::{nodes::StreamLayout, NodeId},
    row::Row,
};
use dbsp::{
    trace::Spine, CollectionHandle, OrdIndexedZSet, OrdZSet, OutputHandle, Stream, UpsertHandle,
};
use derive_more::{IsVariant, Unwrap};
use std::collections::BTreeMap;

pub type RowSet = OrdZSet<Row, i32>;
pub type RowMap = OrdIndexedZSet<Row, Row, i32>;

pub type Inputs = BTreeMap<NodeId, (RowInput, StreamLayout)>;
pub type Outputs = BTreeMap<NodeId, (RowOutput, StreamLayout)>;

#[derive(Debug, Clone)]
pub enum RowZSet {
    Set(RowSet),
    Map(RowMap),
}

#[derive(Clone, IsVariant, Unwrap)]
pub enum RowTrace<C> {
    Set(Stream<C, Spine<RowSet>>),
    Map(Stream<C, Spine<RowMap>>),
}

pub type ZSetHandle = CollectionHandle<Row, i32>;
pub type ZSetUpsertHandle = UpsertHandle<Row, bool>;
pub type IndexedZSetHandle = CollectionHandle<Row, (Row, i32)>;
pub type IndexedZSetUpsertHandle = UpsertHandle<Row, Option<Row>>;

#[derive(Clone, IsVariant, Unwrap)]
pub enum RowInput {
    /// A handle to a zset
    ZSet(ZSetHandle),
    /// A handle to an upsert set
    UpsertZSet(ZSetUpsertHandle),
    /// A handle to an indexed zset (map)
    IndexedZSet(IndexedZSetHandle),
    /// A handle to an upsert indexed zset (map)
    UpsertIndexedZSet(IndexedZSetUpsertHandle),
}

impl RowInput {
    /// Returns `true` if `self` is a [`ZSet`] or [`UpsertZSet`]
    ///
    /// [`ZSet`]: RowInput::ZSet
    /// [`UpsertZSet`]: RowInput::UpsertZSet
    pub const fn is_set(&self) -> bool {
        matches!(self, Self::ZSet(..) | Self::UpsertZSet(..))
    }

    /// Returns `true` if `self` is a [`IndexedZSet`] or [`UpsertIndexedZSet`]
    ///
    /// [`IndexedZSet`]: RowInput::IndexedZSet
    /// [`UpsertIndexedZSet`]: RowInput::UpsertIndexedZSet
    pub const fn is_map(&self) -> bool {
        matches!(self, Self::IndexedZSet(..) | Self::UpsertIndexedZSet(..))
    }

    /// Returns `true` if `self` is a [`ZSet`] or [`IndexedZSet`]
    ///
    /// [`ZSet`]: RowInput::ZSet
    /// [`IndexedZSet`]: RowInput::IndexedZSet
    pub const fn is_upsert(&self) -> bool {
        matches!(self, Self::ZSet(..) | Self::IndexedZSet(..))
    }

    pub const fn as_zset(&self) -> Option<&ZSetHandle> {
        if let Self::ZSet(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub fn as_zset_mut(&mut self) -> Option<&mut ZSetHandle> {
        if let Self::ZSet(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub const fn as_upsert_zset(&self) -> Option<&ZSetUpsertHandle> {
        if let Self::UpsertZSet(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub fn as_upsert_zset_mut(&mut self) -> Option<&mut ZSetUpsertHandle> {
        if let Self::UpsertZSet(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub const fn as_indexed_zset(&self) -> Option<&IndexedZSetHandle> {
        if let Self::IndexedZSet(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub fn as_indexed_zset_mut(&mut self) -> Option<&mut IndexedZSetHandle> {
        if let Self::IndexedZSet(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub const fn as_upsert_indexed_zset(&self) -> Option<&IndexedZSetUpsertHandle> {
        if let Self::UpsertIndexedZSet(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub fn as_upsert_indexed_zset_mut(&mut self) -> Option<&mut IndexedZSetUpsertHandle> {
        if let Self::UpsertIndexedZSet(handle) = self {
            Some(handle)
        } else {
            None
        }
    }
}

#[derive(Clone)]
pub enum RowOutput {
    Set(OutputHandle<RowSet>),
    Map(OutputHandle<RowMap>),
}

impl RowOutput {
    pub const fn as_set(&self) -> Option<&OutputHandle<RowSet>> {
        if let Self::Set(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub fn as_set_mut(&mut self) -> Option<&mut OutputHandle<RowSet>> {
        if let Self::Set(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub const fn as_map(&self) -> Option<&OutputHandle<RowMap>> {
        if let Self::Map(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub fn as_map_mut(&mut self) -> Option<&mut OutputHandle<RowMap>> {
        if let Self::Map(handle) = self {
            Some(handle)
        } else {
            None
        }
    }
}

// TODO: Change the weight to a `Row`? Toggle between `i32` and `i64`?
#[derive(Clone, IsVariant, Unwrap)]
pub enum RowStream<C> {
    Set(Stream<C, RowSet>),
    Map(Stream<C, RowMap>),
}

impl<C> RowStream<C> {
    pub const fn as_set(&self) -> Option<&Stream<C, RowSet>> {
        if let Self::Set(set) = self {
            Some(set)
        } else {
            None
        }
    }

    pub const fn as_map(&self) -> Option<&Stream<C, RowMap>> {
        if let Self::Map(map) = self {
            Some(map)
        } else {
            None
        }
    }
}
