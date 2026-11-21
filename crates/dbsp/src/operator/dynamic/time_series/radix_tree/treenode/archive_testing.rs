//! Construct the persisted rolling COUNT row without exposing the radix internals.

use super::{ChildPtr, TreeNode};
use crate::operator::dynamic::time_series::radix_tree::Prefix;
use crate::utils::{Tup0, Tup1, Tup2};
use rkyv::{Archived, option::ArchivedOption};
use std::mem::{offset_of, size_of};

type Child = ChildPtr<u64, Tup1<i64>>;
type Node = TreeNode<u64, Tup1<i64>>;
type Value = Tup2<Prefix<u64>, Node>;
/// A row in the persisted rolling COUNT radix tree.
pub type RollingCountTreeRow = Tup2<Tup0, Value>;

/// Byte offsets of one optional child from the start of the archived row.
pub struct ChildArchiveOffsets {
    /// Byte offset of the option tag.
    pub tag: usize,
    /// Byte offset of the prefix key.
    pub prefix_key: usize,
    /// Byte offset of the prefix length.
    pub prefix_len: usize,
    /// Byte offset of the aggregate count.
    pub count: usize,
}

/// Byte offsets from the start of an archived rolling COUNT row.
pub struct TreeArchiveOffsets {
    /// Byte offset of the prefix key.
    pub prefix_key: usize,
    /// Byte offset of the prefix length.
    pub prefix_len: usize,
    /// Offsets for the two children, in radix order.
    pub children: [ChildArchiveOffsets; 2],
}

/// Builds a row from its prefix and the prefix and count of each child.
pub fn rolling_count_tree_row(
    prefix: (u64, u32),
    children: [Option<(u64, u32, i64)>; 2],
) -> RollingCountTreeRow {
    let (key, prefix_len) = prefix;
    Tup2(
        Tup0::new(),
        Tup2(
            Prefix { key, prefix_len },
            Node {
                children: children.map(|child| {
                    child.map(|(key, prefix_len, count)| Child {
                        child_prefix: Prefix { key, prefix_len },
                        child_agg: Tup1(count),
                    })
                }),
            },
        ),
    )
}

/// Returns the rkyv field offsets for the current build.
pub fn rolling_count_tree_offsets() -> TreeArchiveOffsets {
    let value = offset_of!(Archived<RollingCountTreeRow>, 1);
    let key = value + offset_of!(Archived<Value>, 0);
    let tree = value + offset_of!(Archived<Value>, 1);
    let children = tree + offset_of!(Archived<Node>, children);
    type ArchivedOptionalChild = Archived<Option<Child>>;
    let example = rkyv::to_bytes::<_, 256>(&Some(Child::default())).unwrap();
    // SAFETY: the buffer is aligned and comes from serializing exactly Option<Child>.
    let archived = unsafe { rkyv::archived_root::<Option<Child>>(&example) };
    let ArchivedOption::Some(child) = archived else {
        unreachable!("serialized Some")
    };
    // ArchivedOption stores the Some tag at the start of the archived option.
    debug_assert_eq!(
        example[example.len() - size_of::<ArchivedOptionalChild>()],
        1
    );
    let payload = std::ptr::from_ref(child) as usize - std::ptr::from_ref(archived) as usize;
    let prefix_key = offset_of!(Archived<Prefix<u64>>, key);
    let prefix_len = offset_of!(Archived<Prefix<u64>>, prefix_len);
    TreeArchiveOffsets {
        prefix_key: key + prefix_key,
        prefix_len: key + prefix_len,
        children: std::array::from_fn(|index| {
            let tag = children + index * size_of::<ArchivedOptionalChild>();
            let child = tag + payload;
            let prefix = child + offset_of!(Archived<Child>, child_prefix);
            ChildArchiveOffsets {
                tag,
                prefix_key: prefix + prefix_key,
                prefix_len: prefix + prefix_len,
                count: child
                    + offset_of!(Archived<Child>, child_agg)
                    + offset_of!(Archived<Tup1<i64>>, 0),
            }
        }),
    }
}
