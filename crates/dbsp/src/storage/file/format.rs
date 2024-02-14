//! # Layer file format
//!
//! A layer file is a sequence of variable-sized binary blocks, each a
//! power-of-2 multiple of 4 kB in length.  The order of the blocks in a file is
//! unspecified, except that the last block in a file is a [`FileTrailer`] block
//! that is exactly 4 kB.
//!
//! The layer file implementation uses [`mod@binrw`] for serializing and
//! deserializing fixed-length data, and [`rkyv`] for serializing and
//! deserializing variable-length data.  The layer file implementation
//! configures [`mod@binrw`] for little-endian input and output.
//!
//! Each block begins with an 8-byte [`BlockHeader`].
//!
//! # Data blocks
//!
//! A data block consists of the following, in order:
//!
//! * [`DataBlockHeader`].
//!
//! * [`Item<K, A>`](`super::Item`) data items serialized with [`rkyv`].  The
//!   number of these is specified in [`DataBlockHeader::n_values`].
//!
//! * The "value map", which holds a byte offset from the start of the block to
//!   the start of each [`Item`](`super::Item`).  The offset to the value map is
//!   specified in [`DataBlockHeader::value_map_ofs`] and the format in
//!   [`DataBlockHeader::value_map_varint`].
//!
//! * The "row groups", which point from a row in this column to the associated
//!   rows in the next column.  The offset to the value map is specified in
//!   [`DataBlockHeader::row_groups_ofs`] and the format in
//!   [`DataBlockHeader::row_group_varint`].  Data blocks in the last column do
//!   not have row groups.
//!
//! # Index blocks
//!
//! An index block consists of the following, in order:
//!
//! * [`IndexBlockHeader`].
//!
//! * A sequence of bounds serialized with [`rkyv`], one pair for each of
//!   [`IndexBlockHeader::n_children`].  The first value in each pair is the
//!   smallest value in the child tree and the second is the largest value.
//!
//! * A "bound map", which holds a byte offset from the start of the block to
//!   the start of each bound.  The offset to the bound map is specified in
//!   [`IndexBlockHeader::bound_map_offset`] and the format in
//!   [`IndexBlockHeader::bound_map_varint`].
//!
//! * An array of "row totals", one for each of
//!   [`IndexBlockHeader::n_children`].  The first row total is the total number
//!   of rows in the first child tree, the second row total is that plus the
//!   total number of rows in the second child tree, and so on.
//!
//! * An array of "child pointers", one for each of
//!   [`IndexBlockHeader::n_children`].  Each one of these points to a child
//!   block.  For `size` and `offset` both in bytes, each pointer is encoded as
//!   `(offset >> 7) | size.trailing_zeros()`, which allows `size` to range up
//!   to `2**31` bytes.

use crate::storage::buffer_cache::FBuf;

use binrw::{binrw, BinRead, BinResult, BinWrite, Error as BinError};
#[cfg(doc)]
use crc32c;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

/// Increment this on each incompatible change.
pub const VERSION_NUMBER: u32 = 1;

/// Magic number for data blocks.
pub const DATA_BLOCK_MAGIC: [u8; 4] = *b"LFDB";

/// Magic number for index blocks.
pub const INDEX_BLOCK_MAGIC: [u8; 4] = *b"LFIB";

/// Magic number for the file trailer block.
pub const FILE_TRAILER_BLOCK_MAGIC: [u8; 4] = *b"LFFT";

/// 8-byte header at the beginning of each block.
///
/// A block does not identify its own size, so any reference to a block must
/// also include the block's size.
#[binrw]
#[derive(Debug)]
pub struct BlockHeader {
    /// 32-bit [`crc32c`] checksum of the remainder of the block.
    pub checksum: u32,

    /// Magic number.  Magic numbers begin with `LF`, which stands for "layer
    /// file".
    pub magic: [u8; 4],
}

impl BlockHeader {
    pub(crate) fn new(magic: &[u8; 4]) -> Self {
        Self {
            checksum: 0,
            magic: *magic,
        }
    }
}

/// File trailer block.
///
/// Padded with zeros to exactly fill a 4-kB block.
///
/// Serialized and deserialized automatically with [`mod@binrw`].
#[binrw]
#[derive(Debug)]
pub struct FileTrailer {
    /// Block header with "LFFT" magic.
    #[brw(assert(header.magic == FILE_TRAILER_BLOCK_MAGIC, "file trailer has bad magic"))]
    pub header: BlockHeader,

    /// Currently, must be [`VERSION_NUMBER`].  In the future, this allows for
    /// detecting version changes and supporting backward compatibility.
    pub version: u32,

    /// Number of columns.
    #[bw(calc(columns.len() as u32))]
    pub n_columns: u32,

    /// The columns.
    #[br(count = n_columns)]
    pub columns: Vec<FileTrailerColumn>,
}

/// Information about a column.
///
/// Embedded inside the [`FileTrailer`] block.
///
/// Serialized and deserialized automatically with [`mod@binrw`].
#[binrw]
#[derive(Debug, Copy, Clone)]
pub struct FileTrailerColumn {
    /// File offset in bytes of the top-level block.  If the column has no rows,
    /// this should be 0.
    pub node_offset: u64,

    /// Length of the top-level block in bytes.  If the column has no rows, this
    /// should be 0.
    pub node_size: u32,

    /// Type of the top-level node.  If the column has no rows, this should be
    /// [`NodeType::Data`].
    #[brw(align_after = 4)]
    pub node_type: NodeType,

    /// Number of rows in the column.  Column 0 may have any number of rows;
    /// subsequent columns must each have more rows than the previous.
    pub n_rows: u64,
}

/// Type of a node in a column B-tree.
///
/// Serialized and deserialized automatically with [`mod@binrw`].
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
#[binrw]
#[brw(repr(u8))]
pub enum NodeType {
    /// A data node that begins with a [`DataBlockHeader`].
    Data = 0,

    /// An index node that begins with a [`IndexBlockHeader`].
    Index = 1,
}

pub(crate) trait FixedLen {
    const LEN: usize;
}

/// Index block header.
///
/// Serialized and deserialized automatically with [`mod@binrw`].
#[binrw]
pub struct IndexBlockHeader {
    /// Block header with "LFIB" magic.
    #[brw(assert(header.magic == INDEX_BLOCK_MAGIC, "index block has bad magic"))]
    pub header: BlockHeader,

    /// Offset, in bytes from the beginning of the block, to the bound map.
    ///
    /// The bound map has `2 * n_children` entries.
    pub bound_map_offset: u32,

    /// Offset, in bytes from the beginning of the block, to the row totals.
    ///
    /// There are [`n_children`](Self::n_children) row totals.
    pub row_totals_offset: u32,

    /// Offset, in bytes from the beginning of the block, to the child pointers.
    ///
    /// There are [`n_children`](Self::n_children) child pointers.
    pub child_pointers_offset: u32,

    /// Number of child nodes.
    pub n_children: u16,

    /// Child node type.  All of the child nodes have the same type.
    pub child_type: NodeType,

    /// The representation of the bound map.
    pub bound_map_varint: Varint,

    /// The representation of the row totals.
    pub row_total_varint: Varint,

    #[brw(align_after = 16)]
    /// The representation of the child pointers.
    pub child_pointer_varint: Varint,
}

impl FixedLen for IndexBlockHeader {
    const LEN: usize = 32;
}

/// Header for each data block.
///
/// Serialized and deserialized automatically with [`mod@binrw`].
#[binrw]
pub struct DataBlockHeader {
    /// Block header with `LFDB` magic.
    #[brw(assert(header.magic == DATA_BLOCK_MAGIC, "data block has bad magic"))]
    pub header: BlockHeader,

    /// Number of values (rows) in the block.
    pub n_values: u32,

    /// Offset, in bytes from the beginning of the block, to the value map.
    pub value_map_ofs: u32,

    /// Offset, in bytes from the beginning of the block, to the row groups.
    pub row_groups_ofs: u32,

    /// The representation of the value map.
    ///
    /// The value map is, logically, an array of [`n_values`](Self::n_values)
    /// integers, in which the `i`th entry is a byte offset from the
    /// beginning of the block to the [`Item<K, A>`](`super::Item`) that
    /// represents the `i` value in the data block.
    ///
    /// The physical representation of the value map depends on
    /// [`value_map_varint`](Self::value_map_varint):
    ///
    /// * When this is `Some(varint)`, [`value_map_ofs`](Self::value_map_ofs)
    ///   points to an array of [`n_values`](Self::n_values) integers, each
    ///   `varint` bytes long, that directly represent the value map.
    ///
    /// * When this is `None`, [`value_map_ofs`](Self::value_map_ofs) points to
    ///   an array of 2 32-bit integers `(start, stride)`.  The `i`th value in
    ///   the value map is then calculated as `start + stride * i`.
    ///
    /// This single-byte value is serialized as either a valid [`Varint`] for
    /// `Some(<value>)` or as a zero byte for `None`.
    #[bw(write_with = Varint::write_opt)]
    #[br(parse_with = Varint::parse_opt)]
    pub value_map_varint: Option<Varint>,

    /// The representation of row groups.
    ///
    /// In columns other than the last column, this value is `Some(varint)` and
    /// the row groups are an array of `n_values + 1` `varint`-byte integers
    /// starting at byte offset `value_map_ofs`(Self::value_map_ofs) within this
    /// block.  The entries with indexes `i` and `i + 1` are the range of rows
    /// in the next column associated with this column's row `i`.
    ///
    /// In the last column, this value is `None` and
    /// [`value_map_ofs`](Self::value_map_ofs) should be 0.
    ///
    /// This single-byte value is serialized as either a valid [`Varint`] for
    /// `Some(<value>)` or as a zero byte for `None`.
    #[bw(write_with = Varint::write_opt)]
    #[br(parse_with = Varint::parse_opt)]
    #[brw(align_after = 16)]
    pub row_group_varint: Option<Varint>,
}

impl FixedLen for DataBlockHeader {
    const LEN: usize = 32;
}

/// Variable-length integer identifier.
///
/// A `Varint` identifies the size of integers in arrays.  This saves space when
/// the integers are small, which is common in practice.  Saving space to reduce
/// overhead is important because it reduces the size and the depth of the index
/// structure.
///
/// To save space, arrays of `Varint`s values aren't aligned.
#[derive(Copy, Clone, PartialEq, Eq, FromPrimitive)]
#[binrw]
#[brw(repr(u8))]
pub enum Varint {
    /// 8-bit integer.
    B8 = 1,
    /// 16-bit integer.
    B16 = 2,
    /// 24-bit integer.
    B24 = 3,
    /// 32-bit integer.
    B32 = 4,
    /// 48-bit integer.
    B48 = 6,
    /// 64-bit integer.
    B64 = 8,
}
impl Varint {
    pub(crate) fn from_max_value(max_value: u64) -> Varint {
        #[allow(clippy::unusual_byte_groupings, clippy::match_overlapping_arm)]
        match max_value {
            ..=0xff => Varint::B8,
            ..=0xffff => Varint::B16,
            ..=0xffff_ff => Varint::B24,
            ..=0xffff_ffff => Varint::B32,
            ..=0xffff_ffff_ffff => Varint::B48,
            _ => Varint::B64,
        }
    }
    pub(crate) fn from_len(len: usize) -> Varint {
        Self::from_max_value(len as u64 - 1)
    }
    pub(crate) fn alignment(&self) -> usize {
        match self {
            Self::B24 => 1,
            Self::B48 => 2,
            _ => *self as usize,
        }
    }
    pub(crate) fn align(&self, offset: usize) -> usize {
        next_multiple_of_pow2(offset, self.alignment())
    }
    pub(crate) fn len(&self) -> usize {
        *self as usize
    }
    pub(crate) fn put(&self, dst: &mut FBuf, value: u64) {
        #[allow(clippy::unnecessary_cast)]
        match *self {
            Self::B8 => dst.push(value as u8),
            Self::B16 => dst.extend_from_slice(&(value as u16).to_le_bytes()),
            Self::B24 => dst.extend_from_slice(&(value as u32).to_le_bytes()[..3]),
            Self::B32 => dst.extend_from_slice(&(value as u32).to_le_bytes()),
            Self::B48 => dst.extend_from_slice(&(value as u64).to_le_bytes()[..6]),
            Self::B64 => dst.extend_from_slice(&(value as u64).to_le_bytes()),
        }
    }
    pub(crate) fn get(&self, src: &FBuf, offset: usize) -> u64 {
        let mut raw = [0u8; 8];
        raw[..self.len()].copy_from_slice(&src[offset..offset + self.len()]);
        u64::from_le_bytes(raw)
    }
    #[binrw::parser(reader, endian)]
    pub(crate) fn parse_opt() -> BinResult<Option<Varint>> {
        let byte: u8 = <_>::read_options(reader, endian, ())?;
        match byte {
            0 => Ok(None),
            _ => match FromPrimitive::from_u8(byte) {
                Some(varint) => Ok(Some(varint)),
                None => Err(BinError::NoVariantMatch {
                    pos: reader.stream_position()? - 1,
                }),
            },
        }
    }
    #[binrw::writer(writer, endian)]
    pub(crate) fn write_opt(value: &Option<Varint>) -> BinResult<()> {
        value
            .map_or(0, |varint| varint as u8)
            .write_options(writer, endian, ())
    }
}

// Rounds up `offset` to the next multiple of `alignment`, which must be a power
// of 2.  This is equivalent to `offset.next_multiple(alignment)` except for the
// assumption about `alignment` being a power of 2, which allows it to be faster
// and smaller in the case where the compiler can't see the power-of-2 property.
fn next_multiple_of_pow2(offset: usize, alignment: usize) -> usize {
    let mask = alignment - 1;
    (offset + mask) & !mask
}

/*
/// Key and auxiliary data.
///
/// This type is serialized with `rkyv` for each value in a data block.
///
/// The definition of this struct makes it appear that the key precedes the
/// auxiliary data, but the Rust compiler can reorder struct members (in both
/// `Item` and [`ArchivedItem`]), so the serialize form might be in the reverse
/// order.
///
/// `rkyv` can serialize arrays, slices, and vectors perfectly well on its own.
/// This crate serializes each value in a data block separately because there is
/// no way to accurately predict how big a serialized vector will be without
/// doing it.
#[derive(Archive, Serialize)]
pub struct Item<'a, K, A>(#[with(Inline)] pub &'a K, #[with(Inline)] pub &'a A)
where
    K: Rkyv,
    A: Rkyv;
*/
