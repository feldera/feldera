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

#[derive(Clone, IsVariant, Unwrap)]
pub enum RowInput {
    ZSet(CollectionHandle<Row, i32>),
    UpsertZSet(UpsertHandle<Row, bool>),
    IndexedZSet(CollectionHandle<Row, (Row, i32)>),
    UpsertIndexedZSet(UpsertHandle<Row, Option<Row>>),
}

impl RowInput {
    pub const fn as_set(&self) -> Option<&CollectionHandle<Row, i32>> {
        if let Self::ZSet(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub fn as_set_mut(&mut self) -> Option<&mut CollectionHandle<Row, i32>> {
        if let Self::ZSet(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub const fn as_map(&self) -> Option<&CollectionHandle<Row, (Row, i32)>> {
        if let Self::IndexedZSet(handle) = self {
            Some(handle)
        } else {
            None
        }
    }

    pub fn as_map_mut(&mut self) -> Option<&mut CollectionHandle<Row, (Row, i32)>> {
        if let Self::IndexedZSet(handle) = self {
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
