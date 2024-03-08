use std::{
    fmt::{self, Debug, Display, Formatter, Write},
    mem::take,
};

use num::PrimInt;
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;

use super::{Prefix, RADIX};
use crate::{
    declare_trait_object,
    dynamic::{Data, DataTrait, DowncastTrait, DynOpt, Erase},
    operator::dynamic::aggregate::AggCombineFunc,
    DBData,
};

/// Pointer to a child node.
#[derive(
    Clone,
    Default,
    Debug,
    SizeOf,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[archive(bound(archive = "Prefix<TS>: DBData"))]
pub struct ChildPtr<TS: DBData, A: DBData> {
    /// Unique prefix of a child subtree, which serves as a pointer
    /// to the child node.  Given this prefix the child node can
    /// be located using `Cursor::seek_key`, unless
    /// `child_prefix.is_leaf()`, in which case we're at the bottom
    /// of the tree.
    child_prefix: Prefix<TS>,
    /// Aggregate over all timestamps covered by the child subtree.
    child_agg: A,
}

impl<TS: DBData, A: DBData> ChildPtr<TS, A> {
    #[cfg(test)]
    fn new(child_prefix: Prefix<TS>, child_agg: A) -> Self {
        Self {
            child_prefix,
            child_agg,
        }
    }
}

impl<TS, A> Display for ChildPtr<TS, A>
where
    TS: DBData + PrimInt,
    A: DBData,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "[{}->{:?}]", self.child_prefix, self.child_agg)
    }
}

pub trait ChildPtrTrait<TS, A>: Data
where
    TS: DBData + PrimInt,
    A: DataTrait + ?Sized,
{
    #[allow(clippy::wrong_self_convention)]
    fn from_timestamp(&mut self, key: TS, child_agg: &mut A);

    #[allow(clippy::wrong_self_convention)]
    fn from_prefix(&mut self, child_prefix: Prefix<TS>);

    fn child_prefix(&self) -> Prefix<TS>;

    fn child_agg(&self) -> &A;

    fn child_agg_mut(&mut self) -> &mut A;
}

impl<TS, A, AType> ChildPtrTrait<TS, A> for ChildPtr<TS, AType>
where
    TS: DBData + PrimInt,
    A: DataTrait + ?Sized,
    AType: DBData + Erase<A>,
{
    fn from_timestamp(&mut self, key: TS, child_agg: &mut A) {
        let child_agg = unsafe { child_agg.downcast_mut::<AType>() };

        self.child_prefix = Prefix::from_timestamp(key);
        self.child_agg = take(child_agg);
    }

    fn from_prefix(&mut self, child_prefix: Prefix<TS>) {
        self.child_prefix = child_prefix;
        self.child_agg = Default::default();
    }

    fn child_prefix(&self) -> Prefix<TS> {
        self.child_prefix.clone()
    }

    fn child_agg(&self) -> &A {
        self.child_agg.erase()
    }

    fn child_agg_mut(&mut self) -> &mut A {
        self.child_agg.erase_mut()
    }
}

declare_trait_object!(DynChildPtr<TS, A> = dyn ChildPtrTrait<TS, A>
where
    A: DataTrait + ?Sized,
    TS: PrimInt + DBData);

/// Radix tree node.
#[derive(
    Clone,
    Debug,
    Default,
    SizeOf,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Archive,
    Serialize,
    Deserialize,
)]
//#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
//#[archive(compare(PartialEq, PartialOrd))]
//#[archive(bound(archive = "[Option<ChildPtr<TS, A>>; RADIX]: DBData"))]
pub struct TreeNode<TS: DBData, A: DBData> {
    /// Array of children.
    // `Option` doesn't introduce space overhead.
    children: [Option<ChildPtr<TS, A>>; RADIX],
}

// For some unknown reason we cant jsut use `#[archive(compare(PartialEq,
// PartialOrd))]` and `#[archive_attr(derive(Clone, Ord, PartialOrd, Eq,
// PartialEq))]` so we define these traits for now. They don't get called so
// leaving it unimplemented!().

impl<TS, A> PartialEq<TreeNode<TS, A>> for ArchivedTreeNode<TS, A>
where
    TS: DBData,
    A: DBData,
    //[Option<ChildPtr<TS, A>>; RADIX]: DBData,
{
    fn eq(&self, _other: &TreeNode<TS, A>) -> bool {
        unimplemented!()
    }
}
impl<TS, A> PartialOrd<TreeNode<TS, A>> for ArchivedTreeNode<TS, A>
where
    TS: DBData,
    A: DBData,
    //[Option<ChildPtr<TS, A>>; RADIX]: DBData,
{
    fn partial_cmp(&self, _other: &TreeNode<TS, A>) -> Option<std::cmp::Ordering> {
        unimplemented!()
    }
}

impl<TS, A> PartialEq for ArchivedTreeNode<TS, A>
where
    TS: DBData,
    A: DBData,
{
    fn eq(&self, _other: &Self) -> bool {
        unimplemented!()
    }
}

impl<TS, A> Eq for ArchivedTreeNode<TS, A>
where
    TS: DBData,
    A: DBData,
{
}

impl<TS, A> PartialOrd for ArchivedTreeNode<TS, A>
where
    TS: DBData,
    A: DBData,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<TS, A> Ord for ArchivedTreeNode<TS, A>
where
    TS: DBData,
    A: DBData,
{
    fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
        unimplemented!()
    }
}

// end of not implemented, hopefully derivable, traits

impl<TS, A> Display for TreeNode<TS, A>
where
    TS: DBData + PrimInt,
    A: DBData,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        for child in self.children.iter() {
            match child {
                None => f.write_char('.')?,
                Some(child) => write!(f, "{child}")?,
            }
        }

        Ok(())
    }
}

pub trait TreeNodeTrait<TS, A>: Data
where
    TS: PrimInt + DBData,
    A: DataTrait + ?Sized,
{
    fn clear(&mut self);

    /// Returns a reference to a child pointer number `slot`.
    fn slot(&self, slot: usize) -> &DynOpt<DynChildPtr<TS, A>>;

    /// Returns a mutable reference to a child pointer number `slot`.
    fn slot_mut(&mut self, slot: usize) -> &mut DynOpt<DynChildPtr<TS, A>>;

    /// Counts the number of non-empty slots.
    fn occupied_slots(&self) -> usize;

    /// The first non-empty slot or `None` if all slots are empty.
    fn first_occupied_slot(&self) -> Option<&DynChildPtr<TS, A>>;

    /// Computes aggregate of the entire subtree under `self` as a
    /// sum of aggregates of its children.
    fn aggregate(&self, combine: &dyn AggCombineFunc<A>, result: &mut DynOpt<A>);

    fn display(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result;
}

impl<TS, A, AType> TreeNodeTrait<TS, A> for TreeNode<TS, AType>
where
    TS: PrimInt + DBData,
    A: DataTrait + ?Sized,
    AType: DBData + Erase<A>,
    //[Option<ChildPtr<TS, AType>>; RADIX]: DBData,
{
    fn clear(&mut self) {
        *self = Default::default();
    }

    fn slot(&self, slot: usize) -> &DynOpt<DynChildPtr<TS, A>> {
        self.children[slot].erase()
    }

    fn slot_mut(&mut self, slot: usize) -> &mut DynOpt<DynChildPtr<TS, A>> {
        self.children[slot].erase_mut()
    }

    fn occupied_slots(&self) -> usize {
        let mut res = 0;
        for child in self.children.iter() {
            if child.is_some() {
                res += 1;
            }
        }
        res
    }

    fn first_occupied_slot(&self) -> Option<&DynChildPtr<TS, A>> {
        if let Some(child) = self.children.iter().flatten().next() {
            return Some(child.erase());
        }
        None
    }

    fn aggregate(&self, combine: &dyn AggCombineFunc<A>, acc: &mut DynOpt<A>) {
        for child in self.children.iter().flatten() {
            if let Some(acc) = acc.get_mut() {
                combine(acc, child.child_agg.erase());
            } else {
                acc.from_ref(child.child_agg.erase());
            }
        }
    }

    fn display(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

declare_trait_object!(DynTreeNode<TS, A> = dyn TreeNodeTrait<TS, A>
where
    TS: PrimInt + DBData,
    A: DataTrait + ?Sized);

impl<TS, A> Display for DynTreeNode<TS, A>
where
    TS: PrimInt + DBData,
    A: DataTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        self.display(f)
    }
}

/// Describes incremental update to a radix tree node.
#[derive(
    Clone,
    Default,
    Debug,
    SizeOf,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound(archive = "Option<TreeNode<TS,A>>: DBData, Prefix<TS>: DBData"))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct TreeNodeUpdate<TS: DBData, A: DBData> {
    /// Prefix that uniquely identifies the node.
    pub prefix: Prefix<TS>,
    /// Old value of the node or `None` if we are creating a new node.
    pub old: Option<TreeNode<TS, A>>,
    /// New value of the node or `None` if we are deleting an existing node.
    pub new: Option<TreeNode<TS, A>>,
}

pub trait TreeNodeUpdateTrait<TS, A>: Data
where
    TS: PrimInt + DBData,
    A: DataTrait + ?Sized,
{
    fn prefix(&self) -> Prefix<TS>;

    fn old(&self) -> &DynOpt<DynTreeNode<TS, A>>;

    fn old_mut(&mut self) -> &mut DynOpt<DynTreeNode<TS, A>>;

    #[allow(clippy::wrong_self_convention, clippy::new_ret_no_self)]
    fn new(&self) -> &DynOpt<DynTreeNode<TS, A>>;

    fn new_mut(&mut self) -> &mut DynOpt<DynTreeNode<TS, A>>;

    #[allow(clippy::wrong_self_convention)]
    fn from_existing_node(&mut self, prefix: Prefix<TS>, node: &mut DynTreeNode<TS, A>);

    #[allow(clippy::wrong_self_convention)]
    fn from_new_node(&mut self, prefix: Prefix<TS>, node: &mut DynTreeNode<TS, A>);
}

impl<TS, A, AType> TreeNodeUpdateTrait<TS, A> for TreeNodeUpdate<TS, AType>
where
    TS: PrimInt + DBData,
    A: DataTrait + ?Sized,
    AType: DBData + Erase<A>,
    //[Option<ChildPtr<TS, AType>>; RADIX]: DBData,
{
    fn prefix(&self) -> Prefix<TS> {
        self.prefix.clone()
    }

    fn old(&self) -> &DynOpt<DynTreeNode<TS, A>> {
        self.old.erase()
    }

    fn old_mut(&mut self) -> &mut DynOpt<DynTreeNode<TS, A>> {
        self.old.erase_mut()
    }

    fn new(&self) -> &DynOpt<DynTreeNode<TS, A>> {
        self.new.erase()
    }

    fn new_mut(&mut self) -> &mut DynOpt<DynTreeNode<TS, A>> {
        self.new.erase_mut()
    }

    fn from_existing_node(&mut self, prefix: Prefix<TS>, node: &mut DynTreeNode<TS, A>) {
        let node = unsafe { node.downcast_mut::<TreeNode<TS, AType>>() };
        self.prefix = prefix;
        self.old = Some(node.clone());
        self.new = Some(take(node));
    }

    fn from_new_node(&mut self, prefix: Prefix<TS>, node: &mut DynTreeNode<TS, A>) {
        let node = unsafe { node.downcast_mut::<TreeNode<TS, AType>>() };

        self.prefix = prefix;
        self.old = None;
        self.new = Some(take(node));
    }
}
declare_trait_object!(DynTreeNodeUpdate<TS, A> = dyn TreeNodeUpdateTrait<TS, A>
where
    A: DataTrait + ?Sized,
    TS: PrimInt + DBData);

#[cfg(test)]
mod test {
    use crate::{
        dynamic::{DowncastTrait, DynData, Erase},
        operator::dynamic::time_series::radix_tree::{
            treenode::{ChildPtr, TreeNode, TreeNodeTrait},
            DynChildPtr, DynTreeNode, Prefix,
        },
        DBData,
    };
    use rkyv::{archived_root, to_bytes, Deserialize, Infallible};

    fn aggregate_default(node: &mut DynTreeNode<u64, DynData /* <u64> */>) -> Option<u64> {
        let mut result: Option<u64> = None;
        node.aggregate(
            &mut |acc, x| *acc.downcast_mut_checked::<u64>() += *x.downcast_checked::<u64>(),
            result.erase_mut(),
        );
        result
    }

    fn child_from_timestamp<A: DBData>(key: u64, mut val: A) -> ChildPtr<u64, A> {
        let mut child: ChildPtr<u64, A> = Default::default();
        let dyn_child: &mut DynChildPtr<u64, DynData /* <A> */> = child.erase_mut();

        dyn_child.from_timestamp(key, val.erase_mut());

        child
    }

    #[test]
    fn test_tree_node() {
        let mut node = TreeNode::<u64, u64>::default();
        let node: &mut DynTreeNode<u64, DynData /* <u64> */> = node.erase_mut();

        assert_eq!(node.occupied_slots(), 0);
        assert_eq!(node.first_occupied_slot(), None);
        assert_eq!(aggregate_default(node), None);

        node.slot_mut(1).set_some_with(&mut |ptr| {
            ptr.from_timestamp(0x1000_0000_0000_0000u64, 10u64.erase_mut())
        });
        assert_eq!(node.occupied_slots(), 1);

        assert_eq!(
            node.first_occupied_slot()
                .unwrap()
                .downcast_checked::<ChildPtr<_, _>>(),
            &child_from_timestamp(0x1000_0000_0000_0000u64, 10u64)
        );

        assert_eq!(aggregate_default(node), Some(10));

        node.slot_mut(4).set_some_with(&mut |ptr| {
            ptr.from_timestamp(0x4000_0000_0000_0000u64, 40u64.erase_mut())
        });
        assert_eq!(node.occupied_slots(), 2);
        assert_eq!(
            node.first_occupied_slot()
                .unwrap()
                .downcast_checked::<ChildPtr<_, _>>(),
            &child_from_timestamp(0x1000_0000_0000_0000u64, 10u64)
        );
        assert_eq!(aggregate_default(node), Some(50));

        node.slot_mut(8).set_some_with(&mut |ptr| {
            *ptr.downcast_mut_checked() = ChildPtr::new(
                Prefix {
                    key: 0x8fff_ffff_ffff_ffffu64,
                    prefix_len: 4,
                },
                80u64,
            )
        });
        assert_eq!(node.occupied_slots(), 3);
        assert_eq!(
            node.first_occupied_slot()
                .unwrap()
                .downcast_checked::<ChildPtr<_, _>>(),
            &child_from_timestamp(0x1000_0000_0000_0000u64, 10u64)
        );
        assert_eq!(aggregate_default(node), Some(130));
    }

    #[test]
    fn childptr_decode_encode() {
        type Type = ChildPtr<u64, i32>;
        for input in [
            child_from_timestamp(u64::MIN, -1),
            child_from_timestamp(0x1000_0000_0000_0000u64, 10),
            child_from_timestamp(u64::MAX, 3),
        ] {
            let input: Type = input;
            let encoded = to_bytes::<_, 4096>(&input).unwrap();
            let archived = unsafe { archived_root::<Type>(&encoded[..]) };
            let decoded: Type = archived.deserialize(&mut Infallible).unwrap();
            assert_eq!(decoded, input);
        }
    }

    #[test]
    fn treenode_decode_encode() {
        type Type = TreeNode<u64, i32>;

        let mut input: Type = TreeNode::default();
        input.slot_mut(1).set_some_with(&mut |ptr: &mut DynChildPtr<
            u64,
            DynData, /* <i32> */
        >| {
            *ptr.downcast_mut_checked() = child_from_timestamp(0x1000_0000_0000_0000u64, 10);
        });

        let encoded = to_bytes::<_, 4096>(&input).unwrap();
        let archived = unsafe { archived_root::<Type>(&encoded[..]) };
        let decoded: Type = archived.deserialize(&mut Infallible).unwrap();
        assert_eq!(decoded, input);
    }
}
